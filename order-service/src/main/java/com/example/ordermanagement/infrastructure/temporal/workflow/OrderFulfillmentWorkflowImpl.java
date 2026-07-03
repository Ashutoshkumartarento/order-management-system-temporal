package com.example.ordermanagement.infrastructure.temporal.workflow;

import com.example.ordermanagement.infrastructure.temporal.activity.*;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.*;

import java.time.Duration;

/**
 * Workflow Implementation: OrderFulfillmentWorkflowImpl
 *
 * ═══════════════════════════════════════════════════════════════════
 * TEMPORAL WORKFLOW IMPLEMENTATION CONSTRAINTS
 * ═══════════════════════════════════════════════════════════════════
 * Workflow code has strict rules because it must be DETERMINISTIC:
 *
 * ❌ NEVER use: System.currentTimeMillis(), UUID.randomUUID(), Random
 *    Use instead: Workflow.currentTimeMillis(), Workflow.newRandom()
 *
 * ❌ NEVER make: HTTP calls, DB calls, System.getenv() inside workflow
 *    Use instead: Activities (these are the side-effect boundary)
 *
 * ❌ NEVER use: Threads, CompletableFuture, synchronized blocks
 *    Use instead: Workflow.async(), Promise, Async.function()
 *
 * WHY DETERMINISTIC?
 * Temporal replays the workflow history on crash recovery.
 * It compares what ACTUALLY happened (from history) with what the code WOULD do.
 * If the code is non-deterministic, replay produces different results → corruption.
 *
 * ═══════════════════════════════════════════════════════════════════
 * SAGA PATTERN IMPLEMENTATION
 * ═══════════════════════════════════════════════════════════════════
 * The Saga pattern manages distributed transactions without 2PC.
 * Instead of rollback, we use COMPENSATING TRANSACTIONS:
 *
 *   Reserve Inventory  →  Process Payment  →  Create Shipment
 *         ↑                    ↑                    ↑
 *   Release Inventory  ←  Refund Payment   ←  [Compensation if shipment fails]
 *
 * Each step records its "undo" action. On failure, we run undo actions
 * in reverse order. Temporal makes this simple and reliable.
 *
 * ═══════════════════════════════════════════════════════════════════
 * RETRY POLICIES
 * ═══════════════════════════════════════════════════════════════════
 * Each activity has its own retry policy:
 * - Inventory: 3 attempts, no non-retryable errors
 * - Payment: 2 attempts (payment providers often have transient failures)
 * - Shipment: 3 attempts
 *
 * Exponential backoff means retries don't hammer downstream services.
 *
 * ═══════════════════════════════════════════════════════════════════
 * SIGNALS AND QUERIES
 * ═══════════════════════════════════════════════════════════════════
 * Signals and queries are processed between workflow steps (at safe points).
 * We use Workflow.await() to pause and wait for a signal.
 */
public class OrderFulfillmentWorkflowImpl implements OrderFulfillmentWorkflow {

    // ─────────────────────────────────────────────────────
    // Activity stubs — Temporal creates proxy objects
    // that automatically serialize calls to the activity task queue
    // ─────────────────────────────────────────────────────

    private final InventoryActivity inventoryActivity = Workflow.newActivityStub(
            InventoryActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(3)
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(2.0)
                            .setMaximumInterval(Duration.ofSeconds(30))
                            .build())
                    .build()
    );

    private final PaymentActivity paymentActivity = Workflow.newActivityStub(
            PaymentActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(60))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(2)
                            .setInitialInterval(Duration.ofSeconds(2))
                            .setBackoffCoefficient(2.0)
                            .setMaximumInterval(Duration.ofSeconds(60))
                            // These errors should NOT be retried automatically.
                            // Temporal matches ApplicationFailure.type against these names.
                            // The SDK uses the simple class name when serializing exceptions.
                            .setDoNotRetry(
                                    "InsufficientFundsException",
                                    "CardDeclinedException"
                            )
                            .build())
                    .build()
    );

    private final ShippingActivity shippingActivity = Workflow.newActivityStub(
            ShippingActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(3)
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(2.0)
                            .build())
                    .build()
    );

    private final NotificationActivity notificationActivity = Workflow.newActivityStub(
            NotificationActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    // Notifications are best-effort — don't retry aggressively
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(2)
                            .build())
                    .build()
    );

    // ─────────────────────────────────────────────────────
    // Mutable workflow state — accessible via Queries
    // ─────────────────────────────────────────────────────

    private String status = "STARTING";
    private String currentStep = "INITIALIZING";
    private int retryCount = 0;
    private boolean cancellationRequested = false;
    private String cancellationReason = "";
    private boolean paymentRetryRequested = false;
    private String failureReason = "";

    // ─────────────────────────────────────────────────────
    // Compensation state — tracks what needs to be undone
    // ─────────────────────────────────────────────────────

    private String reservationId = null;
    private String transactionId = null;
    private String shipmentId = null;

    // ═══════════════════════════════════════════════════════════════════
    // MAIN WORKFLOW METHOD
    // ═══════════════════════════════════════════════════════════════════

    @Override
    public void fulfill(String orderId, long stepDelayMs) {
        // Using Workflow.getLogger() — safe for deterministic replay
        io.temporal.workflow.Workflow.getLogger(OrderFulfillmentWorkflowImpl.class)
                .info("Starting OrderFulfillmentWorkflow for orderId={}", orderId);

        // ──────────────────────────────────────────────────
        // STEP 1: Reserve Inventory
        // ──────────────────────────────────────────────────
        currentStep = "RESERVING_INVENTORY";
        status = "IN_PROGRESS";

        // Pause here: if a cancel signal arrives within stepDelayMs, cancellation
        // is processed before any activity is ever scheduled.
        awaitCancelWindow(stepDelayMs);
        if (shouldCancel()) {
            compensateAndCancel(orderId, cancellationReason, "CUSTOMER");
            return;
        }

        InventoryActivity.ReservationResult reservationResult;
        try {
            reservationResult = inventoryActivity.reserveInventory(orderId);
            reservationId = reservationResult.reservationId();

            // Record domain event via activity callback
            inventoryActivity.recordInventoryReserved(orderId, reservationResult.reservationId());

        } catch (Exception e) {
            // All retries exhausted — record failure and cancel
            inventoryActivity.recordInventoryReservationFailed(orderId, e.getMessage());
            cancelOrderDirectly(orderId, "Inventory unavailable: " + e.getMessage(), "SYSTEM_COMPENSATION");
            status = "FAILED";
            currentStep = "INVENTORY_FAILED";
            failureReason = e.getMessage();
            return;
        }

        // ──────────────────────────────────────────────────
        // STEP 2: Process Payment
        // Inventory reserved — if payment fails, must release inventory
        // ──────────────────────────────────────────────────

        // Pause here so a cancel signal can arrive before payment is scheduled.
        // This is the primary window for "cancel after inventory, before payment."
        currentStep = "PAYMENT_PENDING";
        awaitCancelWindow(stepDelayMs);
        currentStep = "PROCESSING_PAYMENT";

        if (shouldCancel()) {
            // Release reserved inventory before cancelling
            compensateInventory(orderId, "Customer requested cancellation");
            cancelOrderDirectly(orderId, cancellationReason, "CUSTOMER");
            status = "CANCELLED";
            currentStep = "CANCELLED";
            return;
        }

        PaymentActivity.PaymentResult paymentResult;
        boolean paymentSucceeded = false;

        // Allow payment retry via signal — wait up to 5 minutes
        for (int attempt = 0; attempt < 3; attempt++) {
            retryCount = attempt;
            try {
                paymentResult = paymentActivity.processPayment(orderId);
                transactionId = paymentResult.transactionId();

                // Cancel signal may have arrived while the activity was running.
                // Payment was charged — record it first so the domain state allows
                // the refund transition (completeRefund requires PAYMENT_COMPLETED).
                if (shouldCancel()) {
                    paymentActivity.recordPaymentCompleted(orderId, paymentResult.transactionId());
                    compensatePayment(orderId, "Customer cancelled during payment processing");
                    compensateInventory(orderId, "Customer cancelled during payment processing");
                    cancelOrderDirectly(orderId, cancellationReason, "CUSTOMER");
                    status = "CANCELLED";
                    currentStep = "CANCELLED";
                    return;
                }

                paymentActivity.recordPaymentCompleted(orderId, paymentResult.transactionId());
                paymentSucceeded = true;
                break;

            } catch (Exception e) {
                // In Temporal, activity exceptions arrive as ActivityFailure wrapping
                // an ApplicationFailure. We check cause chain by exception type name,
                // since the original Java type may not survive serialization boundaries.
                boolean retryable = isPaymentRetryable(e);

                paymentActivity.recordPaymentFailed(orderId,
                        extractMessage(e), retryable);

                if (!retryable || attempt >= 2) {
                    // No more retries — compensate and cancel
                    compensateInventory(orderId, "Payment failed permanently");
                    cancelOrderDirectly(orderId, "Payment failed: " + e.getMessage(), "SYSTEM_COMPENSATION");
                    status = "FAILED";
                    currentStep = "PAYMENT_FAILED";
                    failureReason = e.getMessage();
                    return;
                }

                // Wait for customer to retry payment via signal (up to 5 minutes)
                currentStep = "WAITING_FOR_PAYMENT_RETRY";
                boolean signalReceived = Workflow.await(
                        Duration.ofMinutes(5),
                        () -> paymentRetryRequested || cancellationRequested
                );

                if (cancellationRequested || !signalReceived) {
                    compensateInventory(orderId, "Customer cancelled or payment retry timed out");
                    cancelOrderDirectly(orderId, cancellationReason.isEmpty()
                            ? "Payment retry timed out" : cancellationReason, "CUSTOMER");
                    return;
                }

                // Reset flag for next attempt
                paymentRetryRequested = false;
                currentStep = "RETRYING_PAYMENT";
            }
        }

        if (!paymentSucceeded) {
            return;
        }

        // ──────────────────────────────────────────────────
        // STEP 3: Create Shipment
        // Payment taken — if shipment fails, must refund + release inventory
        // ──────────────────────────────────────────────────

        // Pause here: cancel before shipment refunds the payment automatically.
        currentStep = "SHIPMENT_PENDING";
        awaitCancelWindow(stepDelayMs);
        currentStep = "CREATING_SHIPMENT";

        if (shouldCancel()) {
            // Too late to cancel without refund
            compensatePayment(orderId, "Customer requested cancellation after payment");
            compensateInventory(orderId, "Customer requested cancellation after payment");
            cancelOrderDirectly(orderId, cancellationReason, "CUSTOMER");
            status = "CANCELLED";
            currentStep = "CANCELLED";
            return;
        }

        ShippingActivity.ShipmentResult shipmentResult;
        try {
            shipmentResult = shippingActivity.createShipment(orderId);
            shipmentId = shipmentResult.shipmentId();
            shippingActivity.recordShipmentCreated(orderId,
                    shipmentResult.shipmentId(),
                    shipmentResult.trackingNumber(),
                    shipmentResult.carrier());

            // Cancel signal may have arrived while createShipment was running.
            // Shipment exists in shipping-service — compensate payment and inventory.
            if (shouldCancel()) {
                compensatePayment(orderId, "Customer cancelled after shipment creation");
                compensateInventory(orderId, "Customer cancelled after shipment creation");
                cancelOrderDirectly(orderId, cancellationReason, "CUSTOMER");
                status = "CANCELLED";
                currentStep = "CANCELLED";
                return;
            }

        } catch (Exception e) {
            // Shipment failed — refund payment, release inventory, cancel
            compensatePayment(orderId, "Shipment creation failed");
            compensateInventory(orderId, "Shipment creation failed");
            cancelOrderDirectly(orderId, "Shipment failed: " + e.getMessage(), "SYSTEM_COMPENSATION");
            status = "FAILED";
            currentStep = "SHIPMENT_FAILED";
            failureReason = e.getMessage();
            return;
        }

        // ──────────────────────────────────────────────────
        // STEP 4: Wait for Delivery
        // In real systems this could wait for a webhook from the carrier
        // ──────────────────────────────────────────────────
        currentStep = "AWAITING_DELIVERY";

        // Simulate delivery confirmation (in reality: wait for external signal)
        shippingActivity.confirmDelivery(orderId, shipmentId);
        shippingActivity.recordShipmentDelivered(orderId, shipmentId);

        // ──────────────────────────────────────────────────
        // STEP 5: Complete — Send notifications
        // ──────────────────────────────────────────────────
        currentStep = "COMPLETED";
        status = "COMPLETED";

        notificationActivity.sendOrderDeliveredNotification(orderId);

        io.temporal.workflow.Workflow.getLogger(OrderFulfillmentWorkflowImpl.class)
                .info("OrderFulfillmentWorkflow completed successfully orderId={}", orderId);
    }

    // ═══════════════════════════════════════════════════════════════════
    // SIGNAL HANDLERS
    // ═══════════════════════════════════════════════════════════════════

    @Override
    public void cancelOrder(String reason) {
        io.temporal.workflow.Workflow.getLogger(OrderFulfillmentWorkflowImpl.class)
                .info("CancelOrder signal received reason={}", reason);
        this.cancellationReason = reason;
        this.cancellationRequested = true;
    }

    @Override
    public void retryPayment() {
        io.temporal.workflow.Workflow.getLogger(OrderFulfillmentWorkflowImpl.class)
                .info("RetryPayment signal received");
        this.paymentRetryRequested = true;
    }

    // ═══════════════════════════════════════════════════════════════════
    // QUERY HANDLERS
    // ═══════════════════════════════════════════════════════════════════

    @Override
    public String getCurrentStatus() {
        return status;
    }

    @Override
    public WorkflowProgress getProgress() {
        return new WorkflowProgress(status, currentStep, retryCount, cancellationRequested, failureReason);
    }

    // ═══════════════════════════════════════════════════════════════════
    // COMPENSATION HELPERS — SAGA PATTERN
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Compensating transaction: Release previously reserved inventory.
     * Called when downstream steps fail.
     */
    private void compensateInventory(String orderId, String reason) {
        if (reservationId == null) return;
        try {
            inventoryActivity.releaseInventory(orderId, reservationId);
            inventoryActivity.recordInventoryReleased(orderId, reason);
        } catch (Exception e) {
            // Even compensation can fail. In production, this would go to
            // a manual intervention queue. Here we log and continue.
            Workflow.getLogger(this.getClass())
                    .error("Failed to release inventory for orderId={}: {}", orderId, e.getMessage());
        }
    }

    /**
     * Compensating transaction: Refund payment.
     * Called when shipment fails after payment was taken.
     */
    private void compensatePayment(String orderId, String reason) {
        if (transactionId == null) return;
        try {
            paymentActivity.refundPayment(orderId, transactionId);
            paymentActivity.recordRefundCompleted(orderId, "REFUND-" + transactionId);
        } catch (Exception e) {
            Workflow.getLogger(this.getClass())
                    .error("Failed to refund payment for orderId={}: {}", orderId, e.getMessage());
        }
    }

    /**
     * Cancels the order via activity (writes domain event).
     */
    private void cancelOrderDirectly(String orderId, String reason, String cancelledBy) {
        try {
            inventoryActivity.recordOrderCancelled(orderId, reason, cancelledBy);
        } catch (Exception e) {
            Workflow.getLogger(this.getClass())
                    .error("Failed to record order cancellation for orderId={}", orderId, e);
        }
    }

    /**
     * Full compensation: release inventory, then cancel.
     */
    private void compensateAndCancel(String orderId, String reason, String cancelledBy) {
        if (reservationId != null) {
            compensateInventory(orderId, reason);
        }
        cancelOrderDirectly(orderId, reason, cancelledBy);
        status = "CANCELLED";
        currentStep = "CANCELLED";
    }

    /**
     * Pauses at a cancel checkpoint for up to stepDelayMs milliseconds.
     * Returns early if a cancel signal arrives — Workflow.await is deterministic
     * and survives worker crash/replay (unlike Thread.sleep).
     */
    private void awaitCancelWindow(long stepDelayMs) {
        if (stepDelayMs > 0) {
            Workflow.await(Duration.ofMillis(stepDelayMs), () -> cancellationRequested);
        }
    }

    private boolean shouldCancel() {
        return cancellationRequested;
    }

    /**
     * Determines if a payment failure is retryable.
     *
     * Temporal wraps activity exceptions. In the test environment the original
     * exception IS preserved in the cause chain. In production, ActivityFailure
     * wraps ApplicationFailure which carries the original type name as a string.
     * We check both Java instanceof and type-name string matching for robustness.
     */
    private boolean isPaymentRetryable(Throwable e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof InsufficientFundsException || t instanceof CardDeclinedException) {
                return false;
            }
            // Temporal's ApplicationFailure carries the original class name in getType()
            if (t instanceof io.temporal.failure.ApplicationFailure af) {
                String type = af.getType();
                if (type != null && (type.contains("InsufficientFunds") || type.contains("CardDeclined"))) {
                    return false;
                }
            }
            t = t.getCause();
        }
        return true;
    }

    /**
     * Extracts the most descriptive message from a potentially wrapped exception.
     */
    private String extractMessage(Throwable e) {
        Throwable t = e;
        while (t != null) {
            if (t.getMessage() != null && !t.getMessage().isBlank()) {
                return t.getMessage();
            }
            t = t.getCause();
        }
        return e.getClass().getSimpleName();
    }

    /**
     * Unwraps Temporal's ActivityFailure/ApplicationFailure wrapper to find the root cause.
     */
    private Throwable unwrapCause(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
