package com.example.ordermanagement.infrastructure.temporal.workflow;

import com.example.ordermanagement.infrastructure.temporal.activity.*;
import com.example.ordermanagement.infrastructure.temporal.workflow.model.WorkflowDefinition;
import com.example.ordermanagement.infrastructure.temporal.workflow.model.WorkflowStep;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;

public class OrderFulfillmentWorkflowImpl implements OrderFulfillmentWorkflow {

    private String status = "STARTING";
    private String currentStep = "INITIALIZING";
    private int retryCount = 0;
    private boolean cancellationRequested = false;
    private String cancellationReason = "";
    private boolean paymentRetryRequested = false;
    private String failureReason = "";

    // Compensation state
    private String reservationId = null;
    private String transactionId = null;
    private String shipmentId = null;

    private NotificationActivity notificationActivity;

    private java.util.Stack<WorkflowStep> executedSteps = new java.util.Stack<>();

    @Override
    public void fulfill(String orderId, long stepDelayMs, WorkflowDefinition workflowDefinition) {
        Workflow.getLogger(OrderFulfillmentWorkflowImpl.class).info("Starting YAML-based OrderFulfillmentWorkflow for orderId={}", orderId);

        // Setup generic notification activity
        notificationActivity = Workflow.newActivityStub(NotificationActivity.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(10))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        for (WorkflowStep step : workflowDefinition.steps()) {
            awaitCancelWindow(stepDelayMs);
            if (shouldCancel()) {
                compensateAndCancel(orderId, cancellationReason, "CUSTOMER");
                return;
            }

            currentStep = step.name();
            status = "IN_PROGRESS";

            try {
                executeStep(step, orderId);
                executedSteps.push(step);
            } catch (Exception e) {
                if (step.name().equals("PROCESS_PAYMENT")) {
                    boolean retryable = isPaymentRetryable(e);
                    try {
                        PaymentActivity paymentActivity = Workflow.newActivityStub(PaymentActivity.class, ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());
                        paymentActivity.recordPaymentFailed(orderId, extractMessage(e), retryable);
                    } catch (Exception ex) {}
                    
                    if (retryable) {
                        currentStep = "WAITING_FOR_PAYMENT_RETRY";
                        boolean signalReceived = Workflow.await(Duration.ofMinutes(5), () -> paymentRetryRequested || cancellationRequested);
                        if (cancellationRequested || !signalReceived) {
                            compensateAndCancel(orderId, cancellationReason.isEmpty() ? "Payment retry timed out" : cancellationReason, "CUSTOMER");
                            return;
                        }
                        paymentRetryRequested = false;
                        try {
                            executeStep(step, orderId);
                            executedSteps.push(step);
                        } catch (Exception e2) {
                            handleFailure(orderId, step, e2);
                            return;
                        }
                        continue;
                    }
                }
                handleFailure(orderId, step, e);
                return;
            }
        }

        currentStep = "AWAITING_DELIVERY";
        ShippingActivity shippingActivity = Workflow.newActivityStub(ShippingActivity.class, ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());
        shippingActivity.confirmDelivery(orderId, shipmentId);
        shippingActivity.recordShipmentDelivered(orderId, shipmentId);

        currentStep = "COMPLETED";
        status = "COMPLETED";
        notificationActivity.sendOrderDeliveredNotification(orderId);
        Workflow.getLogger(OrderFulfillmentWorkflowImpl.class).info("OrderFulfillmentWorkflow completed successfully orderId={}", orderId);
    }

    private void executeStep(WorkflowStep step, String orderId) {
        ActivityOptions options = ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(step.timeoutSeconds()))
                .setRetryOptions(RetryOptions.newBuilder()
                        .setMaximumAttempts(step.maxAttempts())
                        .setInitialInterval(Duration.ofSeconds(1))
                        .setBackoffCoefficient(2.0)
                        .build())
                .build();

        switch (step.name()) {
            case "RESERVE_INVENTORY":
                InventoryActivity inventoryActivity = Workflow.newActivityStub(InventoryActivity.class, options);
                InventoryActivity.ReservationResult reservationResult = inventoryActivity.reserveInventory(orderId);
                reservationId = reservationResult.reservationId();
                inventoryActivity.recordInventoryReserved(orderId, reservationId);
                break;
            case "PROCESS_PAYMENT":
                PaymentActivity paymentActivity = Workflow.newActivityStub(PaymentActivity.class, options);
                PaymentActivity.PaymentResult paymentResult = paymentActivity.processPayment(orderId);
                transactionId = paymentResult.transactionId();
                paymentActivity.recordPaymentCompleted(orderId, transactionId);
                break;
            case "CREATE_SHIPMENT":
                ShippingActivity shippingActivity = Workflow.newActivityStub(ShippingActivity.class, options);
                ShippingActivity.ShipmentResult shipmentResult = shippingActivity.createShipment(orderId);
                shipmentId = shipmentResult.shipmentId();
                shippingActivity.recordShipmentCreated(orderId, shipmentId, shipmentResult.trackingNumber(), shipmentResult.carrier());
                break;
            default:
                throw new IllegalArgumentException("Unknown step: " + step.name());
        }
    }

    private void handleFailure(String orderId, WorkflowStep step, Exception e) {
        ActivityOptions options = ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build();
        
        if (step.name().equals("RESERVE_INVENTORY")) {
            try {
                InventoryActivity inventoryActivity = Workflow.newActivityStub(InventoryActivity.class, options);
                inventoryActivity.recordInventoryReservationFailed(orderId, extractMessage(e));
            } catch (Exception ex) {}
        } else if (step.name().equals("PROCESS_PAYMENT")) {
            try {
                PaymentActivity paymentActivity = Workflow.newActivityStub(PaymentActivity.class, options);
                paymentActivity.recordPaymentFailed(orderId, extractMessage(e), false);
            } catch (Exception ex) {}
        }

        compensateAll(orderId);
        status = "FAILED";
        currentStep = step.name() + "_FAILED";
        failureReason = e.getMessage();
        try {
            InventoryActivity inventoryActivity = Workflow.newActivityStub(InventoryActivity.class, options);
            inventoryActivity.recordOrderCancelled(orderId, "Step " + step.name() + " failed: " + e.getMessage(), "SYSTEM_COMPENSATION");
        } catch (Exception ex) {
            // Ignore
        }
    }

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

    private void compensateAll(String orderId) {
        ActivityOptions options = ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build();
        while (!executedSteps.isEmpty()) {
            WorkflowStep step = executedSteps.pop();
            String compMethod = step.compensationMethod();
            if (compMethod != null) {
                switch (compMethod) {
                    case "releaseInventory":
                        if (reservationId != null) {
                            InventoryActivity inventoryActivity = Workflow.newActivityStub(InventoryActivity.class, options);
                            inventoryActivity.releaseInventory(orderId, reservationId);
                            inventoryActivity.recordInventoryReleased(orderId, "Compensated");
                        }
                        break;
                    case "refundPayment":
                        if (transactionId != null) {
                            PaymentActivity paymentActivity = Workflow.newActivityStub(PaymentActivity.class, options);
                            paymentActivity.refundPayment(orderId, transactionId);
                            paymentActivity.recordRefundCompleted(orderId, "REFUND-" + transactionId);
                        }
                        break;
                    default:
                        Workflow.getLogger(OrderFulfillmentWorkflowImpl.class).warn("Unknown compensation method: {}", compMethod);
                        break;
                }
            }
        }
    }

    private void compensateAndCancel(String orderId, String reason, String cancelledBy) {
        compensateAll(orderId);
        try {
            InventoryActivity inventoryActivity = Workflow.newActivityStub(InventoryActivity.class, ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());
            inventoryActivity.recordOrderCancelled(orderId, reason, cancelledBy);
        } catch (Exception e) {}
        status = "CANCELLED";
        currentStep = "CANCELLED";
    }

    @Override
    public void cancelOrder(String reason) {
        this.cancellationReason = reason;
        this.cancellationRequested = true;
    }

    @Override
    public void retryPayment() {
        this.paymentRetryRequested = true;
    }

    @Override
    public String getCurrentStatus() {
        return status;
    }

    @Override
    public WorkflowProgress getProgress() {
        return new WorkflowProgress(status, currentStep, retryCount, cancellationRequested, failureReason);
    }

    private void awaitCancelWindow(long stepDelayMs) {
        if (stepDelayMs > 0) {
            Workflow.await(Duration.ofMillis(stepDelayMs), () -> cancellationRequested);
        }
    }

    private boolean shouldCancel() {
        return cancellationRequested;
    }

    private boolean isPaymentRetryable(Throwable e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof InsufficientFundsException || t instanceof CardDeclinedException) {
                return false;
            }
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
}
