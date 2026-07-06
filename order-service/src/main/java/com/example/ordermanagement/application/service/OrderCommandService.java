package com.example.ordermanagement.application.service;

import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.command.*;
import com.example.ordermanagement.domain.exception.OrderNotFoundException;
import com.example.ordermanagement.domain.model.OrderStatus;
import com.example.ordermanagement.domain.port.inbound.OrderCommandUseCase;
import com.example.ordermanagement.domain.port.outbound.OrderRepository;
import com.example.ordermanagement.domain.port.outbound.WorkflowPort;
import com.example.ordermanagement.domain.valueobject.OrderId;
import com.example.ordermanagement.domain.valueobject.OrderItem;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Application Service: OrderCommandService
 *
 * ═══════════════════════════════════════════════════════════════════
 * RESPONSIBILITY
 * ═══════════════════════════════════════════════════════════════════
 * Application services are the USE CASE layer in Hexagonal Architecture.
 * They coordinate:
 *   1. Loading aggregates from the repository (event replay)
 *   2. Delegating business logic to domain objects
 *   3. Persisting resulting events
 *   4. Calling external systems (Temporal) via output ports
 *   5. Transaction management
 *
 * ═══════════════════════════════════════════════════════════════════
 * NO BUSINESS LOGIC HERE
 * ═══════════════════════════════════════════════════════════════════
 * All business rules live in the Order aggregate.
 * This service is "thin" — it orchestrates, does not decide.
 *
 * ═══════════════════════════════════════════════════════════════════
 * INTERACTION WITH TEMPORAL
 * ═══════════════════════════════════════════════════════════════════
 * When an order is confirmed:
 *   1. A workflowId is generated HERE (before the workflow starts)
 *   2. The workflowId is stored in the OrderConfirmedEvent
 *   3. Then the Temporal workflow is started via WorkflowPort
 *
 * WHY GENERATE workflowId HERE?
 * Idempotency: if the app crashes after saving the event but before
 * starting the workflow, on retry we find the workflowId in the event
 * and can check if the workflow already exists.
 *
 * ═══════════════════════════════════════════════════════════════════
 * IDEMPOTENCY
 * ═══════════════════════════════════════════════════════════════════
 * Temporal activities call back into this service to record domain events.
 * If an activity is retried (worker crash), it may call us twice.
 * We guard against this using the event store's version check:
 *   - If the event is already recorded at that version, we skip it
 * Production systems should also use idempotency keys on incoming requests.
 */
@Service
public class OrderCommandService implements OrderCommandUseCase {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandService.class);

    private final OrderRepository orderRepository;
    private final WorkflowPort workflowPort;
    private final MeterRegistry meterRegistry;

    public OrderCommandService(OrderRepository orderRepository, WorkflowPort workflowPort,
                             MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.workflowPort = workflowPort;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Creates a new Order and persists the initial OrderCreatedEvent.
     * Returns the new OrderId for the caller to use.
     */
    @Transactional
    public OrderId createOrder(CreateOrderCommand command) {
        Timer.Sample timer = Timer.start(meterRegistry);

        try {
            log.info("Creating order for customer {}", command.customerId());

            Order order = Order.create(command.orderId(), command.customerId(), command.shippingAddress());
            orderRepository.save(order);

            meterRegistry.counter("orders.created").increment();
            log.info("Order created: {}", order.getId());
            return order.getId();

        } finally {
            timer.stop(meterRegistry.timer("orders.create.duration"));
        }
    }

    /**
     * Adds an item to a draft order.
     */
    @Transactional
    public void addItem(AddItemCommand command) {
        log.debug("Adding item {} to order {}", command.productId(), command.orderId());

        Order order = loadOrder(command.orderId());

        OrderItem item = new OrderItem(
                command.productId(),
                command.productName(),
                command.quantity(),
                command.unitPrice()
        );

        order.addItem(item);
        orderRepository.save(order);

        log.debug("Item {} added to order {}", command.productId(), command.orderId());
    }

    /**
     * Removes an item from a draft order.
     */
    @Transactional
    public void removeItem(RemoveItemCommand command) {
        Order order = loadOrder(command.orderId());
        order.removeItem(command.productId());
        orderRepository.save(order);
    }

    /**
     * Confirms the order and starts the Temporal fulfillment workflow.
     *
     * WORKFLOW INITIATION PATTERN:
     *   1. Generate workflowId (deterministic from orderId for idempotency)
     *   2. Record OrderConfirmedEvent with workflowId
     *   3. Start Temporal workflow
     *   4. If step 3 fails and is retried, Temporal handles duplicate start gracefully
     */
    @Transactional
    public void confirmOrder(ConfirmOrderCommand command) {
        log.info("Confirming order {}", command.orderId());

        Order order = loadOrder(command.orderId());

        // Generate workflow ID derived from orderId for idempotency
        // Using a prefix + orderId ensures the workflowId is globally unique in Temporal
        String workflowId = "order-fulfillment-" + command.orderId().toString();

        order.confirm(workflowId);
        orderRepository.save(order);

        // Start Temporal workflow AFTER events are persisted
        // If the app crashes here, the workflow isn't started yet.
        // A reconciliation job would detect orders in CONFIRMED state without
        // an active workflow and restart them. (Production concern.)
        workflowPort.startFulfillmentWorkflow(command.orderId(), workflowId);

        meterRegistry.counter("orders.confirmed").increment();
        log.info("Order {} confirmed, workflow {} started", command.orderId(), workflowId);
    }

    /**
     * Cancels an order.
     * If the order has an active workflow, delegates entirely to Temporal via signal —
     * the workflow owns domain state transitions and will call recordOrderCancelled
     * after running compensation (refund, inventory release).
     * Writing CANCELLED directly here while a workflow is mid-flight causes the
     * workflow's subsequent recordPaymentCompleted / recordPaymentFailed activities
     * to hit invalid state transitions in the aggregate.
     */
    @Transactional
    public void cancelOrder(CancelOrderCommand command) {
        log.info("Cancelling order {}: {}", command.orderId(), command.reason());

        Order order = loadOrder(command.orderId());
        String workflowId = order.getWorkflowId();

        // Workflow is running only when workflowId is set AND order is in a non-terminal state.
        // workflowId stays on the order permanently even after the workflow finishes, so we
        // cannot use workflowId alone to decide — a DELIVERED order still has its workflowId set.
        boolean workflowRunning = workflowId != null
                && order.getStatus() != OrderStatus.DELIVERED
                && order.getStatus() != OrderStatus.CANCELLED;

        if (workflowRunning) {
            // Delegate entirely to Temporal — the workflow owns compensation and the CANCELLED
            // domain transition. Writing CANCELLED here while a workflow is mid-flight causes
            // subsequent workflow activities (recordPaymentCompleted etc.) to hit invalid
            // state transitions in the aggregate.
            workflowPort.sendCancelSignal(workflowId, command.reason());
        } else {
            // No active workflow (pre-confirmation, already delivered, or workflow failed) —
            // apply the domain transition directly.
            order.cancel(command.reason(), "CUSTOMER");
            orderRepository.save(order);
        }

        meterRegistry.counter("orders.cancelled.customer").increment();
    }

    // ═══════════════════════════════════════════════════════════════════
    // METHODS CALLED BY TEMPORAL ACTIVITIES
    // These are called back from the infrastructure layer after each
    // Temporal activity completes. They record domain events.
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Records inventory reservation after Temporal InventoryActivity succeeds.
     * Called from InventoryActivity.reserveInventory()
     */
    @Transactional
    public void recordInventoryReserved(OrderId orderId, String reservationId) {
        log.debug("Recording InventoryReserved for order {}, reservationId={}", orderId, reservationId);
        Order order = loadOrder(orderId);
        order.reserveInventory(reservationId);
        orderRepository.save(order);
    }

    /**
     * Records inventory reservation failure.
     * Called from InventoryActivity when all retries are exhausted.
     */
    @Transactional
    public void recordInventoryReservationFailed(OrderId orderId, String reason) {
        log.warn("Recording InventoryReservationFailed for order {}: {}", orderId, reason);
        Order order = loadOrder(orderId);
        order.failInventoryReservation(reason);
        orderRepository.save(order);
        meterRegistry.counter("inventory.reservation.failed").increment();
    }

    /**
     * Records inventory release (compensation).
     */
    @Transactional
    public void recordInventoryReleased(OrderId orderId, String reason) {
        log.debug("Recording InventoryReleased for order {}: {}", orderId, reason);
        Order order = loadOrder(orderId);
        order.releaseInventory(reason);
        orderRepository.save(order);
    }

    /**
     * Records payment completion.
     * Called from PaymentActivity when payment succeeds.
     */
    @Transactional
    public void recordPaymentCompleted(OrderId orderId, String transactionId) {
        log.debug("Recording PaymentCompleted for order {}, txId={}", orderId, transactionId);
        Order order = loadOrder(orderId);
        order.completePayment(transactionId, order.getTotalAmount());
        orderRepository.save(order);
        meterRegistry.counter("payments.completed").increment();
    }

    /**
     * Records payment failure.
     */
    @Transactional
    public void recordPaymentFailed(OrderId orderId, String reason, boolean retryable) {
        log.warn("Recording PaymentFailed for order {}: {} (retryable={})", orderId, reason, retryable);
        Order order = loadOrder(orderId);
        order.failPayment(reason, retryable);
        orderRepository.save(order);
        meterRegistry.counter("payments.failed").increment();
    }

    /**
     * Records refund completion (compensation after shipment failure).
     */
    @Transactional
    public void recordRefundCompleted(OrderId orderId, String refundTxId) {
        log.debug("Recording RefundCompleted for order {}, refundTxId={}", orderId, refundTxId);
        Order order = loadOrder(orderId);
        order.completeRefund(refundTxId, order.getTotalAmount());
        orderRepository.save(order);
    }

    /**
     * Records shipment creation.
     */
    @Transactional
    public void recordShipmentCreated(OrderId orderId, String shipmentId, String trackingNumber, String carrier) {
        log.debug("Recording ShipmentCreated for order {}, shipmentId={}, tracking={}", orderId, shipmentId, trackingNumber);
        Order order = loadOrder(orderId);
        order.createShipment(shipmentId, trackingNumber, carrier);
        orderRepository.save(order);
    }

    /**
     * Records delivery confirmation.
     */
    @Transactional
    public void recordOrderDelivered(OrderId orderId, String shipmentId) {
        log.debug("Recording OrderDelivered for order {}, shipmentId={}", orderId, shipmentId);
        Order order = loadOrder(orderId);
        // Guard: already delivered (Temporal and Kafka paths may both arrive)
        if (order.getStatus().name().equals("DELIVERED")) {
            return;
        }
        order.deliverOrder(shipmentId);
        orderRepository.save(order);
        meterRegistry.counter("orders.delivered").increment();
    }

    /**
     * Records final order cancellation (from saga compensation).
     */
    @Transactional
    public void recordOrderCancelled(OrderId orderId, String reason, String cancelledBy) {
        log.info("Recording OrderCancelled for order {}, reason={}, by={}", orderId, reason, cancelledBy);
        Order order = loadOrder(orderId);
        // Guard: if already cancelled, skip (idempotency)
        if (order.getStatus().name().equals("CANCELLED")) {
            log.debug("Order {} already CANCELLED — skipping (idempotent)", orderId);
            return;
        }
        order.cancel(reason, cancelledBy);
        orderRepository.save(order);
        meterRegistry.counter("orders.cancelled.system").increment();
    }

    // ─────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────

    private Order loadOrder(OrderId orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId.toString()));
    }
}
