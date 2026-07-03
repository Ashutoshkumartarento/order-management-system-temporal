package com.example.ordermanagement.application.service;

import com.example.ordermanagement.api.dto.response.OrderSummaryResponse;
import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.event.DomainEvent;
import com.example.ordermanagement.domain.exception.OrderNotFoundException;
import com.example.ordermanagement.domain.port.inbound.OrderQueryUseCase;
import com.example.ordermanagement.domain.port.outbound.EventStore;
import com.example.ordermanagement.domain.port.outbound.OrderRepository;
import com.example.ordermanagement.domain.port.outbound.WorkflowPort;
import com.example.ordermanagement.domain.valueobject.OrderId;
import com.example.ordermanagement.infrastructure.projection.OrderSummaryRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Application Service: OrderQueryService
 *
 * ═══════════════════════════════════════════════════════════════════
 * CQRS — QUERY SIDE
 * ═══════════════════════════════════════════════════════════════════
 * In CQRS, we separate read models from write models.
 * The write side (OrderCommandService) uses the domain aggregate.
 * The read side (this service) can use optimized read models.
 *
 * For this PoC, we read from the event store directly for simplicity.
 * In production, you'd maintain a separate READ model (a view table or
 * a projection stored in a read-optimized store) that's updated by
 * listening to the event stream.
 *
 * QUERY SIDE IS READ-ONLY:
 * No @Transactional(readOnly = false), no state changes.
 * Queries cannot fail the system — they only read.
 *
 * INTERACTION WITH TEMPORAL:
 * For workflow queries, we call the WorkflowPort which queries Temporal's
 * in-memory workflow state. This is fast and doesn't need event replay.
 */
@Service
public class OrderQueryService implements OrderQueryUseCase {

    private final OrderRepository orderRepository;
    private final EventStore eventStore;
    private final WorkflowPort workflowPort;
    private final OrderSummaryRepository summaryRepository;

    public OrderQueryService(OrderRepository orderRepository, EventStore eventStore,
                             WorkflowPort workflowPort, OrderSummaryRepository summaryRepository) {
        this.orderRepository = orderRepository;
        this.eventStore = eventStore;
        this.workflowPort = workflowPort;
        this.summaryRepository = summaryRepository;
    }

    /**
     * Returns the current state of an order by replaying its events.
     * Loads snapshot if available for performance.
     */
    @Transactional(readOnly = true)
    public Order getOrder(OrderId orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId.toString()));
    }

    /**
     * Returns the complete event history of an order.
     * This is the "audit log" feature of event sourcing.
     *
     * Why is this useful?
     * Traditional CRUD: "The order is CANCELLED" — you have no idea why.
     * Event Sourcing: See every OrderCreated, ItemAdded, PaymentFailed,
     *   InventoryReserved, OrderCancelled event with timestamps and reasons.
     */
    @Transactional(readOnly = true)
    public List<DomainEvent> getOrderHistory(OrderId orderId) {
        if (!eventStore.exists(orderId.toString())) {
            throw new OrderNotFoundException(orderId.toString());
        }
        return eventStore.loadEvents(orderId.toString());
    }

    /**
     * Returns a timeline view of order events for display purposes.
     * Same data as getOrderHistory but structured for human-readable output.
     */
    @Transactional(readOnly = true)
    public List<TimelineEntry> getOrderTimeline(OrderId orderId) {
        return getOrderHistory(orderId).stream()
                .map(event -> new TimelineEntry(
                        event.version(),
                        event.eventType(),
                        event.occurredAt().toString(),
                        describeEvent(event)
                ))
                .toList();
    }

    /**
     * Queries the Temporal workflow for its current execution status.
     * This reads Temporal's internal state, not the event store.
     *
     * TEMPORAL QUERY vs EVENT STORE QUERY:
     * - Temporal query: reflects current workflow execution point (fast, real-time)
     * - Event store query: reflects persisted domain events (authoritative source)
     * Both are valid and complementary.
     */
    public WorkflowPort.WorkflowStatusResult getWorkflowStatus(String workflowId) {
        return workflowPort.queryWorkflowStatus(workflowId);
    }

    @Transactional(readOnly = true)
    public Page<OrderSummaryResponse> listOrders(List<String> statuses, String customerId, Pageable pageable) {
        return summaryRepository.findAll(
                (statuses == null || statuses.isEmpty()) ? null : statuses,
                customerId,
                pageable);
    }

    private String describeEvent(DomainEvent event) {
        return switch (event) {
            case com.example.ordermanagement.domain.event.OrderCreatedEvent e ->
                    "Order created for customer " + e.customerId();
            case com.example.ordermanagement.domain.event.ItemAddedEvent e ->
                    "Item added: " + e.item().productName() + " x" + e.item().quantity();
            case com.example.ordermanagement.domain.event.ItemRemovedEvent e ->
                    "Item removed: productId=" + e.productId();
            case com.example.ordermanagement.domain.event.OrderConfirmedEvent e ->
                    "Order confirmed. Total: " + e.totalAmount() + ". Workflow: " + e.workflowId();
            case com.example.ordermanagement.domain.event.InventoryReservedEvent e ->
                    "Inventory reserved. ReservationId: " + e.reservationId();
            case com.example.ordermanagement.domain.event.InventoryReservationFailedEvent e ->
                    "Inventory reservation FAILED: " + e.reason();
            case com.example.ordermanagement.domain.event.InventoryReleasedEvent e ->
                    "Inventory released (compensation). Reason: " + e.reason();
            case com.example.ordermanagement.domain.event.PaymentCompletedEvent e ->
                    "Payment completed. Transaction: " + e.transactionId() + " Amount: " + e.amountCharged();
            case com.example.ordermanagement.domain.event.PaymentFailedEvent e ->
                    "Payment FAILED: " + e.reason() + (e.retryable() ? " (retryable)" : " (not retryable)");
            case com.example.ordermanagement.domain.event.RefundCompletedEvent e ->
                    "Refund completed. Amount: " + e.amountRefunded();
            case com.example.ordermanagement.domain.event.ShipmentCreatedEvent e ->
                    "Shipment created. Tracking: " + e.trackingNumber() + " via " + e.carrier();
            case com.example.ordermanagement.domain.event.ShipmentDeliveredEvent e ->
                    "Order delivered at " + e.deliveredAt();
            case com.example.ordermanagement.domain.event.OrderCancelledEvent e ->
                    "Order CANCELLED by " + e.cancelledBy() + ": " + e.cancellationReason();
        };
    }

}
