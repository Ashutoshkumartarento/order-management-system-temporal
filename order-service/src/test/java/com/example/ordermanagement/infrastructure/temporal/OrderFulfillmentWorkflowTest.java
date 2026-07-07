package com.example.ordermanagement.infrastructure.temporal;

import com.example.ordermanagement.infrastructure.temporal.activity.*;
import com.example.ordermanagement.infrastructure.temporal.workflow.OrderFulfillmentWorkflow;
import com.example.ordermanagement.infrastructure.temporal.workflow.OrderFulfillmentWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Temporal Workflow Tests
 *
 * ═══════════════════════════════════════════════════════════════════
 * HOW TEMPORAL WORKFLOW TESTING WORKS
 * ═══════════════════════════════════════════════════════════════════
 * Temporal provides TestWorkflowEnvironment — an in-memory server
 * that runs locally without a Docker/Temporal server.
 *
 * Key features:
 * - Time is virtual: fast-forward time to test timeouts
 * - Activities can be replaced with test doubles (POJOs implementing the interface)
 * - Signals and queries work exactly as in production
 *
 * IMPORTANT: Temporal does NOT support Mockito mocks for activities.
 * Activities must be real objects implementing the activity interface.
 * We use anonymous class instances as test doubles here — they're clear
 * and don't require any mocking framework.
 *
 * TESTING APPROACH:
 * 1. Implement activities as simple POJOs controlling behavior via flags
 * 2. Start workflow execution
 * 3. Optionally send signals / fast-forward time
 * 4. Assert workflow completion and state via queries
 */
@DisplayName("OrderFulfillmentWorkflow Tests")
class OrderFulfillmentWorkflowTest {

    private TestWorkflowEnvironment testEnv;
    private Worker worker;
    private WorkflowClient client;

    // ─────────────────────────────────────────────────────
    // Controllable activity test doubles
    // ─────────────────────────────────────────────────────

    private TestInventoryActivity inventoryActivity;
    private TestPaymentActivity paymentActivity;
    private TestShippingActivity shippingActivity;
    private TestNotificationActivity notificationActivity;

    @BeforeEach
    void setUp() {
        testEnv = TestWorkflowEnvironment.newInstance();
        client = testEnv.getWorkflowClient();

        worker = testEnv.newWorker("TEST_QUEUE");
        worker.registerWorkflowImplementationTypes(OrderFulfillmentWorkflowImpl.class);

        // Create fresh test doubles for each test
        inventoryActivity = new TestInventoryActivity();
        paymentActivity = new TestPaymentActivity();
        shippingActivity = new TestShippingActivity();
        notificationActivity = new TestNotificationActivity();

        worker.registerActivitiesImplementations(
                inventoryActivity, paymentActivity, shippingActivity, notificationActivity);

        testEnv.start();
    }

    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    @Test
    @DisplayName("Happy path: Order fulfills successfully end-to-end")
    void shouldFulfillOrderSuccessfully() throws Exception {
        String orderId = UUID.randomUUID().toString();

        OrderFulfillmentWorkflow workflow = startWorkflow(orderId);

        // Wait for workflow to complete
        testEnv.sleep(Duration.ofSeconds(10));

        // Verify activities were called
        assertThat(inventoryActivity.reserveCallCount.get()).isEqualTo(1);
        assertThat(inventoryActivity.reservedRecordCount.get()).isEqualTo(1);
        assertThat(paymentActivity.processCallCount.get()).isEqualTo(1);
        assertThat(paymentActivity.completedRecordCount.get()).isEqualTo(1);
        assertThat(shippingActivity.createCallCount.get()).isEqualTo(1);
        assertThat(shippingActivity.deliveredRecordCount.get()).isEqualTo(1);
        assertThat(notificationActivity.deliveredCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Inventory failure: Compensate and cancel order")
    void shouldCompensateOnInventoryFailure() {
        String orderId = UUID.randomUUID().toString();

        // Configure inventory to always fail
        inventoryActivity.shouldFail = true;

        startWorkflow(orderId);
        testEnv.sleep(Duration.ofMinutes(3)); // Let retries exhaust

        // Compensation: failure recorded, order cancelled
        assertThat(inventoryActivity.failedRecordCount.get()).isGreaterThan(0);
        assertThat(inventoryActivity.cancelledRecordCount.get()).isEqualTo(1);

        // Payment should never have been called
        assertThat(paymentActivity.processCallCount.get()).isEqualTo(0);
    }

    @Test
    @DisplayName("Payment failure after inventory: Release inventory and cancel")
    void shouldCompensateOnPaymentFailure() {
        String orderId = UUID.randomUUID().toString();

        // Inventory succeeds, payment always fails with NON-retryable error.
        // InsufficientFundsException bypasses the 5-min await, triggering
        // compensation immediately (attempt 0, retryable=false → short-circuit).
        paymentActivity.shouldFail = true;
        paymentActivity.failWithTransient = false;

        startWorkflow(orderId);
        // Short sleep — non-retryable failure compensates without waiting for signals
        testEnv.sleep(Duration.ofSeconds(10));

        // Compensation: inventory reserved, then released after payment failure
        assertThat(inventoryActivity.reserveCallCount.get()).isEqualTo(1);
        assertThat(inventoryActivity.releaseCallCount.get()).isGreaterThanOrEqualTo(1);
        assertThat(inventoryActivity.cancelledRecordCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Cancel signal before inventory: Cancel immediately")
    void shouldHandleCancelSignalBeforeInventory() {
        String orderId = UUID.randomUUID().toString();

        // Slow inventory so we can send signal during it
        inventoryActivity.latencyMs = 3000;

        OrderFulfillmentWorkflow workflow = startWorkflow(orderId);

        // Send cancel signal immediately
        workflow.cancelOrder("Test cancellation");

        testEnv.sleep(Duration.ofSeconds(10));

        // Order should be cancelled
        assertThat(inventoryActivity.cancelledRecordCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Query: getCurrentStatus returns workflow state")
    void shouldReturnCurrentStatusViaQuery() {
        String orderId = UUID.randomUUID().toString();

        // Make inventory slow so we can query mid-execution
        inventoryActivity.latencyMs = 3000;

        OrderFulfillmentWorkflow workflow = startWorkflow(orderId);

        // Sleep briefly then query
        testEnv.sleep(Duration.ofMillis(500));

        String status = workflow.getCurrentStatus();
        // May be STARTING or IN_PROGRESS depending on timing
        assertThat(status).isNotNull().isNotEmpty();

        OrderFulfillmentWorkflow.WorkflowProgress progress = workflow.getProgress();
        assertThat(progress).isNotNull();
        assertThat(progress.cancellationRequested()).isFalse();
    }

    @Test
    @DisplayName("Shipment failure: Refund payment, release inventory, cancel")
    void shouldCompensateOnShipmentFailure() {
        String orderId = UUID.randomUUID().toString();

        // Inventory and payment succeed, shipment fails
        shippingActivity.shouldFail = true;

        startWorkflow(orderId);
        testEnv.sleep(Duration.ofMinutes(3));

        // Full compensation chain
        assertThat(paymentActivity.refundCallCount.get()).isEqualTo(1);
        assertThat(inventoryActivity.releaseCallCount.get()).isEqualTo(1);
        assertThat(inventoryActivity.cancelledRecordCount.get()).isEqualTo(1);
    }

    // ─────────────────────────────────────────────────────
    // Test double activity implementations
    // These implement the activity interfaces directly — no mocking framework
    // ─────────────────────────────────────────────────────

    static class TestInventoryActivity implements InventoryActivity {
        volatile boolean shouldFail = false;
        volatile int latencyMs = 100;
        AtomicInteger reserveCallCount = new AtomicInteger();
        AtomicInteger releaseCallCount = new AtomicInteger();
        AtomicInteger reservedRecordCount = new AtomicInteger();
        AtomicInteger failedRecordCount = new AtomicInteger();
        AtomicInteger cancelledRecordCount = new AtomicInteger();

        @Override
        public ReservationResult reserveInventory(String orderId) {
            reserveCallCount.incrementAndGet();
            sleep(latencyMs);
            if (shouldFail) throw new RuntimeException("Inventory unavailable (test)");
            return new ReservationResult("RES-TEST-" + orderId.substring(0, 6), "Reserved");
        }

        @Override
        public void releaseInventory(String orderId, String reservationId) {
            releaseCallCount.incrementAndGet();
        }

        @Override
        public void recordInventoryReserved(String orderId, String reservationId) {
            reservedRecordCount.incrementAndGet();
        }

        @Override
        public void recordInventoryReservationFailed(String orderId, String reason) {
            failedRecordCount.incrementAndGet();
        }

        @Override
        public void recordInventoryReleased(String orderId, String reason) { }

        @Override
        public void recordOrderCancelled(String orderId, String reason, String cancelledBy) {
            cancelledRecordCount.incrementAndGet();
        }
    }

    static class TestPaymentActivity implements PaymentActivity {
        volatile boolean shouldFail = false;
        volatile boolean failWithTransient = false;
        AtomicInteger processCallCount = new AtomicInteger();
        AtomicInteger completedRecordCount = new AtomicInteger();
        AtomicInteger refundCallCount = new AtomicInteger();

        @Override
        public PaymentResult processPayment(String orderId) {
            processCallCount.incrementAndGet();
            if (shouldFail) {
                if (failWithTransient) throw new RuntimeException("Payment timeout (transient)");
                throw new InsufficientFundsException("Insufficient funds (test)");
            }
            return new PaymentResult("TXN-TEST-" + orderId.substring(0, 6), "Paid");
        }

        @Override
        public void refundPayment(String orderId, String transactionId) {
            refundCallCount.incrementAndGet();
        }

        @Override
        public void recordPaymentCompleted(String orderId, String transactionId) {
            completedRecordCount.incrementAndGet();
        }

        @Override
        public void recordPaymentFailed(String orderId, String reason, boolean retryable) { }

        @Override
        public void recordRefundCompleted(String orderId, String refundTransactionId) { }
    }

    static class TestShippingActivity implements ShippingActivity {
        volatile boolean shouldFail = false;
        AtomicInteger createCallCount = new AtomicInteger();
        AtomicInteger deliveredRecordCount = new AtomicInteger();

        @Override
        public ShipmentResult createShipment(String orderId) {
            createCallCount.incrementAndGet();
            if (shouldFail) throw new RuntimeException("Shipment service down (test)");
            return new ShipmentResult("SHIP-TEST", "TRACK-TEST", "UPS");
        }

        @Override
        public void confirmDelivery(String orderId, String shipmentId) { }

        @Override
        public void recordShipmentCreated(String orderId, String shipmentId, String trackingNumber, String carrier) { }

        @Override
        public void recordShipmentDelivered(String orderId, String shipmentId) {
            deliveredRecordCount.incrementAndGet();
        }
    }

    static class TestNotificationActivity implements NotificationActivity {
        AtomicInteger deliveredCount = new AtomicInteger();

        @Override
        public void sendOrderConfirmedNotification(String orderId) { }

        @Override
        public void sendOrderShippedNotification(String orderId, String trackingNumber) { }

        @Override
        public void sendOrderDeliveredNotification(String orderId) {
            deliveredCount.incrementAndGet();
        }

        @Override
        public void sendOrderCancelledNotification(String orderId, String reason) { }

        @Override
        public void sendPaymentFailedNotification(String orderId, String reason) { }
    }

    // ─────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────

    private OrderFulfillmentWorkflow startWorkflow(String orderId) {
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId("test-workflow-" + orderId)
                .setTaskQueue("TEST_QUEUE")
                .build();

        OrderFulfillmentWorkflow workflow = client.newWorkflowStub(
                OrderFulfillmentWorkflow.class, options);

        com.example.ordermanagement.infrastructure.temporal.workflow.YamlWorkflowLoader loader = new com.example.ordermanagement.infrastructure.temporal.workflow.YamlWorkflowLoader();
        com.example.ordermanagement.infrastructure.temporal.workflow.model.WorkflowDefinition definition = loader.loadWorkflow("workflow-config.yml");

        WorkflowClient.start(workflow::fulfill, orderId, 0L, definition);
        return workflow;
    }

    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
