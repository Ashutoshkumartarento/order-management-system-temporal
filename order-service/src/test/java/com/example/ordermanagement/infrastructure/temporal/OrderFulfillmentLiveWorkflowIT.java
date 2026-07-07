package com.example.ordermanagement.infrastructure.temporal;

import com.example.ordermanagement.infrastructure.temporal.activity.*;
import com.example.ordermanagement.infrastructure.temporal.workflow.OrderFulfillmentWorkflow;
import com.example.ordermanagement.infrastructure.temporal.workflow.OrderFulfillmentWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live Temporal Integration Test
 *
 * This test class connects to a REAL Temporal server (expected at localhost:7233)
 * and runs the exact same scenarios as OrderFulfillmentWorkflowTest.
 * Workflows executed here WILL be visible in your Temporal UI (localhost:8233).
 *
 * It starts a local Worker connecting to the real server, but registers the 
 * "Test Double" activities so we can simulate failures (like Inventory failure).
 */
@Disabled("Remove this annotation when your local Temporal server is running via Docker to execute these tests.")
@DisplayName("Live Temporal Workflow Tests")
class OrderFulfillmentLiveWorkflowIT {

    private static final String TASK_QUEUE = "LIVE_TEST_QUEUE_" + UUID.randomUUID().toString().substring(0, 8);
    private WorkflowServiceStubs service;
    private WorkflowClient client;
    private WorkerFactory factory;

    private TestInventoryActivity inventoryActivity;
    private TestPaymentActivity paymentActivity;
    private TestShippingActivity shippingActivity;
    private TestNotificationActivity notificationActivity;

    @BeforeEach
    void setUp() {
        // Connect to the real Temporal server (default localhost:7233)
        service = WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget("127.0.0.1:7233").build());
        client = WorkflowClient.newInstance(service);
        factory = WorkerFactory.newInstance(client);

        // Create a worker specifically for this test's task queue
        Worker worker = factory.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(OrderFulfillmentWorkflowImpl.class);

        // Create fresh test doubles
        inventoryActivity = new TestInventoryActivity();
        paymentActivity = new TestPaymentActivity();
        shippingActivity = new TestShippingActivity();
        notificationActivity = new TestNotificationActivity();

        worker.registerActivitiesImplementations(
                inventoryActivity, paymentActivity, shippingActivity, notificationActivity);

        factory.start();
    }

    @AfterEach
    void tearDown() {
        if (factory != null) {
            factory.shutdown();
        }
        if (service != null) {
            service.shutdown();
        }
    }

    @Test
    @DisplayName("Happy path: Order fulfills successfully end-to-end")
    void shouldFulfillOrderSuccessfully() throws Exception {
        String orderId = UUID.randomUUID().toString();
        OrderFulfillmentWorkflow workflow = startWorkflow(orderId);

        // Wait for workflow to complete (real time now, no skipping)
        Thread.sleep(2000);

        assertThat(inventoryActivity.reserveCallCount.get()).isEqualTo(1);
        assertThat(paymentActivity.processCallCount.get()).isEqualTo(1);
        assertThat(shippingActivity.createCallCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Inventory failure: Compensate and cancel order")
    void shouldCompensateOnInventoryFailure() throws Exception {
        String orderId = UUID.randomUUID().toString();

        // Configure inventory to always fail
        inventoryActivity.shouldFail = true;

        startWorkflow(orderId);
        
        // Wait for 3 retries (1s + 2s + 4s approx)
        Thread.sleep(8000);

        assertThat(inventoryActivity.failedRecordCount.get()).isGreaterThan(0);
        assertThat(inventoryActivity.cancelledRecordCount.get()).isEqualTo(1);
        assertThat(paymentActivity.processCallCount.get()).isEqualTo(0);
    }

    // ─────────────────────────────────────────────────────
    // Test double activity implementations
    // ─────────────────────────────────────────────────────

    static class TestInventoryActivity implements InventoryActivity {
        volatile boolean shouldFail = false;
        AtomicInteger reserveCallCount = new AtomicInteger();
        AtomicInteger releaseCallCount = new AtomicInteger();
        AtomicInteger failedRecordCount = new AtomicInteger();
        AtomicInteger cancelledRecordCount = new AtomicInteger();

        @Override
        public ReservationResult reserveInventory(String orderId) {
            reserveCallCount.incrementAndGet();
            if (shouldFail) throw new RuntimeException("Inventory unavailable (test)");
            return new ReservationResult("RES-TEST-" + orderId.substring(0, 6), "Reserved");
        }

        @Override
        public void releaseInventory(String orderId, String reservationId) {
            releaseCallCount.incrementAndGet();
        }

        @Override
        public void recordInventoryReserved(String orderId, String reservationId) { }

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
        AtomicInteger processCallCount = new AtomicInteger();
        AtomicInteger refundCallCount = new AtomicInteger();

        @Override
        public PaymentResult processPayment(String orderId) {
            processCallCount.incrementAndGet();
            return new PaymentResult("TXN-TEST-" + orderId.substring(0, 6), "Paid");
        }

        @Override
        public void refundPayment(String orderId, String transactionId) {
            refundCallCount.incrementAndGet();
        }

        @Override
        public void recordPaymentCompleted(String orderId, String transactionId) { }

        @Override
        public void recordPaymentFailed(String orderId, String reason, boolean retryable) { }

        @Override
        public void recordRefundCompleted(String orderId, String refundTransactionId) { }
    }

    static class TestShippingActivity implements ShippingActivity {
        AtomicInteger createCallCount = new AtomicInteger();

        @Override
        public ShipmentResult createShipment(String orderId) {
            createCallCount.incrementAndGet();
            return new ShipmentResult("SHIP-TEST", "TRACK-TEST", "UPS");
        }

        @Override
        public void confirmDelivery(String orderId, String shipmentId) { }

        @Override
        public void recordShipmentCreated(String orderId, String shipmentId, String trackingNumber, String carrier) { }

        @Override
        public void recordShipmentDelivered(String orderId, String shipmentId) { }
    }

    static class TestNotificationActivity implements NotificationActivity {
        @Override
        public void sendOrderConfirmedNotification(String orderId) { }

        @Override
        public void sendOrderShippedNotification(String orderId, String trackingNumber) { }

        @Override
        public void sendOrderDeliveredNotification(String orderId) { }

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
                .setWorkflowId("live-test-workflow-" + orderId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        OrderFulfillmentWorkflow workflow = client.newWorkflowStub(
                OrderFulfillmentWorkflow.class, options);

        com.example.ordermanagement.infrastructure.temporal.workflow.YamlWorkflowLoader loader = new com.example.ordermanagement.infrastructure.temporal.workflow.YamlWorkflowLoader();
        com.example.ordermanagement.infrastructure.temporal.workflow.model.WorkflowDefinition definition = loader.loadWorkflow("workflow-config.yml");

        WorkflowClient.start(workflow::fulfill, orderId, 0L, definition);
        return workflow;
    }
}
