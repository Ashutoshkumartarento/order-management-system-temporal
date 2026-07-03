package com.example.ordermanagement.domain.aggregate;

import com.example.ordermanagement.domain.event.*;
import com.example.ordermanagement.domain.exception.DomainException;
import com.example.ordermanagement.domain.exception.InvalidStateTransitionException;
import com.example.ordermanagement.domain.exception.OptimisticLockingException;
import com.example.ordermanagement.domain.model.OrderStatus;
import com.example.ordermanagement.domain.model.PaymentStatus;
import com.example.ordermanagement.domain.model.ShipmentStatus;
import com.example.ordermanagement.domain.valueobject.*;

import java.util.*;

/**
 * Aggregate Root: Order
 *
 * ═══════════════════════════════════════════════════════════════════
 * WHAT IS AN AGGREGATE ROOT?
 * ═══════════════════════════════════════════════════════════════════
 * An Aggregate Root is the entry point to a cluster of domain objects.
 * All changes to the order and its items MUST go through this class.
 * Nothing should modify Order's internal state from outside.
 *
 * ═══════════════════════════════════════════════════════════════════
 * EVENT SOURCING PATTERN
 * ═══════════════════════════════════════════════════════════════════
 * This aggregate follows the Event Sourcing pattern:
 *
 *   1. COMMAND comes in (e.g., AddItemCommand)
 *   2. Aggregate validates business rules (can we add items in this state?)
 *   3. If valid, create a DomainEvent (ItemAddedEvent)
 *   4. Apply the event to update internal state (apply(event))
 *   5. The event is added to pendingEvents list (NOT yet persisted)
 *   6. The application service saves pending events and clears them
 *
 * This means state is NEVER mutated directly — only through events.
 *
 *   ┌─────────────┐     ┌──────────────┐     ┌──────────────────┐
 *   │   Command   │────▶│  Aggregate   │────▶│  Domain Events   │
 *   └─────────────┘     │  (validate)  │     │  (state change)  │
 *                       └──────────────┘     └──────────────────┘
 *                                                     │
 *                                                     ▼
 *                                            ┌────────────────┐
 *                                            │  Event Store   │
 *                                            │  (PostgreSQL)  │
 *                                            └────────────────┘
 *
 * ═══════════════════════════════════════════════════════════════════
 * OPTIMISTIC LOCKING
 * ═══════════════════════════════════════════════════════════════════
 * The 'version' field implements optimistic locking.
 * When loading: we record the current version
 * When saving:  we check that version in DB == our loaded version
 * If another request modified the aggregate concurrently, the versions
 * won't match and we throw OptimisticLockingException.
 *
 * This prevents "lost updates" without pessimistic DB locks.
 *
 * ═══════════════════════════════════════════════════════════════════
 * INTERACTION WITH TEMPORAL
 * ═══════════════════════════════════════════════════════════════════
 * Temporal does NOT call the aggregate directly.
 * Temporal calls Activities → Activities call Application Services →
 * Application Services call this aggregate via commands.
 *
 * The aggregate is ONLY aware of its domain — it knows nothing about
 * Temporal workflows. This separation is intentional.
 */
public class Order {

    // ─────────────────────────────────────────────────────
    // Identity and versioning
    // ─────────────────────────────────────────────────────

    private OrderId id;
    private long version = 0;

    // ─────────────────────────────────────────────────────
    // Domain state
    // ─────────────────────────────────────────────────────

    private CustomerId customerId;
    private OrderStatus status;
    private List<OrderItem> items = new ArrayList<>();
    private Money totalAmount = Money.ZERO;
    private PaymentStatus paymentStatus = PaymentStatus.PENDING;
    private ShipmentStatus shipmentStatus = ShipmentStatus.NOT_CREATED;
    private String shippingAddress;
    private String workflowId;
    private String reservationId;
    private String transactionId;
    private String shipmentId;
    private String trackingNumber;

    // ─────────────────────────────────────────────────────
    // Event sourcing infrastructure
    // ─────────────────────────────────────────────────────

    /**
     * Pending events that have been raised but not yet persisted.
     * The application service drains this list and writes to the event store.
     * This pattern is called "domain event collection".
     */
    private final List<DomainEvent> pendingEvents = new ArrayList<>();

    // ─────────────────────────────────────────────────────
    // Private constructor — force use of factory methods
    // ─────────────────────────────────────────────────────

    private Order() {}

    // ═══════════════════════════════════════════════════════════════════
    // COMMAND HANDLERS — Public API of the aggregate
    // These validate business rules and raise events.
    // They NEVER directly mutate state — that happens only in apply().
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Creates a new Order.
     * This is a STATIC factory — an Order cannot exist without this event.
     *
     * Why static? Because there is no Order instance to call yet.
     * The event IS the creation of the aggregate.
     */
    public static Order create(OrderId orderId, CustomerId customerId, String shippingAddress) {
        if (shippingAddress == null || shippingAddress.isBlank()) {
            throw new DomainException("Shipping address is required");
        }

        Order order = new Order();
        // version starts at 0, first event makes it 1
        OrderCreatedEvent event = OrderCreatedEvent.create(orderId, customerId, shippingAddress, 1L);
        order.applyAndRecord(event);
        return order;
    }

    /**
     * Adds an item to a DRAFT order.
     * Business rules:
     *   - Order must be in DRAFT status
     *   - Duplicate products are merged (quantity increased)
     */
    public void addItem(OrderItem item) {
        requireStatus(OrderStatus.DRAFT, "add items to");

        // Check if product already in order — merge quantities
        boolean duplicate = items.stream()
                .anyMatch(existing -> existing.productId().equals(item.productId()));

        if (duplicate) {
            // Create a remove + re-add event to update quantity cleanly
            // (In production, use a dedicated UpdateItemQuantityEvent instead)
            OrderItem existing = items.stream()
                    .filter(i -> i.productId().equals(item.productId()))
                    .findFirst()
                    .orElseThrow();
            OrderItem merged = existing.withQuantity(existing.quantity() + item.quantity());
            ItemRemovedEvent removeEvent = ItemRemovedEvent.create(id, item.productId(), version + 1);
            applyAndRecord(removeEvent);
            ItemAddedEvent addEvent = ItemAddedEvent.create(id, merged, version + 1);
            applyAndRecord(addEvent);
        } else {
            ItemAddedEvent event = ItemAddedEvent.create(id, item, version + 1);
            applyAndRecord(event);
        }
    }

    /**
     * Removes an item from a DRAFT order.
     */
    public void removeItem(UUID productId) {
        requireStatus(OrderStatus.DRAFT, "remove items from");

        boolean exists = items.stream().anyMatch(i -> i.productId().equals(productId));
        if (!exists) {
            throw new DomainException("Product " + productId + " not found in order");
        }

        ItemRemovedEvent event = ItemRemovedEvent.create(id, productId, version + 1);
        applyAndRecord(event);
    }

    /**
     * Confirms the order, triggering the fulfillment workflow.
     * Business rules:
     *   - Must be in DRAFT status
     *   - Must have at least one item
     *   - workflowId is provided externally (from Temporal, before workflow starts)
     */
    public void confirm(String workflowId) {
        requireStatus(OrderStatus.DRAFT, "confirm");

        if (items.isEmpty()) {
            throw new DomainException("Cannot confirm an empty order");
        }

        Money total = calculateTotal();
        if (total.isZero()) {
            throw new DomainException("Order total cannot be zero");
        }

        OrderConfirmedEvent event = OrderConfirmedEvent.create(id, total, workflowId, version + 1);
        applyAndRecord(event);
    }

    /**
     * Records that inventory was successfully reserved.
     * Called by the application service after Temporal's InventoryActivity succeeds.
     */
    public void reserveInventory(String reservationId) {
        requireStatus(OrderStatus.CONFIRMED, "reserve inventory for");

        InventoryReservedEvent event = InventoryReservedEvent.create(id, reservationId, version + 1);
        applyAndRecord(event);
    }

    /**
     * Records inventory reservation failure.
     */
    public void failInventoryReservation(String reason) {
        requireStatus(OrderStatus.CONFIRMED, "fail inventory reservation for");

        InventoryReservationFailedEvent event = InventoryReservationFailedEvent.create(id, reason, version + 1);
        applyAndRecord(event);
    }

    /**
     * Records inventory release (compensation step).
     * Allowed from any state where inventory was previously reserved —
     * compensation may run after payment or shipment steps have already advanced the status.
     */
    public void releaseInventory(String reason) {
        if (status != OrderStatus.INVENTORY_RESERVED &&
            status != OrderStatus.PAYMENT_PROCESSING &&
            status != OrderStatus.PAYMENT_COMPLETED &&
            status != OrderStatus.SHIPPED) {
            throw new InvalidStateTransitionException(
                    "Cannot release inventory for order in status: " + status);
        }
        InventoryReleasedEvent event = InventoryReleasedEvent.create(id, reservationId, reason, version + 1);
        applyAndRecord(event);
    }

    /**
     * Records successful payment.
     */
    public void completePayment(String transactionId, Money amountCharged) {
        requireStatus(OrderStatus.INVENTORY_RESERVED, "process payment for");

        PaymentCompletedEvent event = PaymentCompletedEvent.create(id, transactionId, amountCharged, version + 1);
        applyAndRecord(event);
    }

    /**
     * Records payment failure.
     */
    public void failPayment(String reason, boolean retryable) {
        if (status != OrderStatus.INVENTORY_RESERVED &&
            status != OrderStatus.PAYMENT_PROCESSING) {
            throw new InvalidStateTransitionException(
                    "Cannot fail payment for order in status: " + status);
        }
        PaymentFailedEvent event = PaymentFailedEvent.create(id, reason, retryable, version + 1);
        applyAndRecord(event);
    }

    /**
     * Records refund completion (compensation step after payment or shipment failure).
     * Allowed from PAYMENT_COMPLETED (shipment/cancel after payment) and SHIPPED
     * (cancel after createShipment but before delivery).
     */
    public void completeRefund(String refundTxId, Money amountRefunded) {
        if (status != OrderStatus.PAYMENT_COMPLETED &&
            status != OrderStatus.SHIPPED) {
            throw new InvalidStateTransitionException(
                    "Cannot refund payment for order in status: " + status);
        }
        RefundCompletedEvent event = RefundCompletedEvent.create(id, refundTxId, amountRefunded, version + 1);
        applyAndRecord(event);
    }

    /**
     * Records shipment creation.
     */
    public void createShipment(String shipmentId, String trackingNumber, String carrier) {
        requireStatus(OrderStatus.PAYMENT_COMPLETED, "create shipment for");

        ShipmentCreatedEvent event = ShipmentCreatedEvent.create(id, shipmentId, trackingNumber, carrier, version + 1);
        applyAndRecord(event);
    }

    /**
     * Records delivery confirmation.
     */
    public void deliverOrder(String shipmentId) {
        requireStatus(OrderStatus.SHIPPED, "deliver");

        ShipmentDeliveredEvent event = ShipmentDeliveredEvent.create(id, shipmentId, version + 1);
        applyAndRecord(event);
    }

    /**
     * Cancels the order — can be called from any non-terminal state.
     */
    public void cancel(String reason, String cancelledBy) {
        if (status == OrderStatus.DELIVERED || status == OrderStatus.CANCELLED) {
            throw new InvalidStateTransitionException(
                    "Cannot cancel an order in terminal state: " + status);
        }
        OrderCancelledEvent event = OrderCancelledEvent.create(id, reason, cancelledBy, version + 1);
        applyAndRecord(event);
    }

    // ═══════════════════════════════════════════════════════════════════
    // EVENT APPLICATION — apply() methods
    // These are the ONLY place where state is mutated.
    // They must be side-effect free (no DB calls, no HTTP calls).
    // They are called during both command handling AND event replay.
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Dispatches an event to the correct apply() method.
     * Uses Java 21 pattern matching switch — exhaustive by the sealed interface.
     *
     * WHY PATTERN MATCHING?
     * If a new event type is added to the sealed DomainEvent interface,
     * the compiler forces you to handle it here. No silent omissions.
     */
    public void apply(DomainEvent event) {
        switch (event) {
            case OrderCreatedEvent e              -> applyOrderCreated(e);
            case ItemAddedEvent e                 -> applyItemAdded(e);
            case ItemRemovedEvent e               -> applyItemRemoved(e);
            case OrderConfirmedEvent e            -> applyOrderConfirmed(e);
            case InventoryReservedEvent e         -> applyInventoryReserved(e);
            case InventoryReservationFailedEvent e -> applyInventoryReservationFailed(e);
            case InventoryReleasedEvent e         -> applyInventoryReleased(e);
            case PaymentCompletedEvent e          -> applyPaymentCompleted(e);
            case PaymentFailedEvent e             -> applyPaymentFailed(e);
            case RefundCompletedEvent e           -> applyRefundCompleted(e);
            case ShipmentCreatedEvent e           -> applyShipmentCreated(e);
            case ShipmentDeliveredEvent e         -> applyShipmentDelivered(e);
            case OrderCancelledEvent e            -> applyOrderCancelled(e);
        }
        // Version always advances with each event
        this.version = event.version();
    }

    private void applyOrderCreated(OrderCreatedEvent event) {
        this.id = OrderId.of(event.aggregateId());
        this.customerId = event.customerId();
        this.status = OrderStatus.DRAFT;
        this.shippingAddress = event.shippingAddress();
    }

    private void applyItemAdded(ItemAddedEvent event) {
        this.items.add(event.item());
        this.totalAmount = calculateTotal();
    }

    private void applyItemRemoved(ItemRemovedEvent event) {
        this.items.removeIf(item -> item.productId().equals(event.productId()));
        this.totalAmount = calculateTotal();
    }

    private void applyOrderConfirmed(OrderConfirmedEvent event) {
        this.status = OrderStatus.CONFIRMED;
        this.totalAmount = event.totalAmount();
        this.workflowId = event.workflowId();
    }

    private void applyInventoryReserved(InventoryReservedEvent event) {
        this.status = OrderStatus.INVENTORY_RESERVED;
        this.reservationId = event.reservationId();
    }

    private void applyInventoryReservationFailed(InventoryReservationFailedEvent event) {
        // Status stays CONFIRMED; order will be cancelled in next event
    }

    private void applyInventoryReleased(InventoryReleasedEvent event) {
        this.reservationId = null;
    }

    private void applyPaymentCompleted(PaymentCompletedEvent event) {
        this.status = OrderStatus.PAYMENT_COMPLETED;
        this.paymentStatus = PaymentStatus.COMPLETED;
        this.transactionId = event.transactionId();
    }

    private void applyPaymentFailed(PaymentFailedEvent event) {
        this.paymentStatus = PaymentStatus.FAILED;
        this.status = OrderStatus.PAYMENT_PROCESSING;
    }

    private void applyRefundCompleted(RefundCompletedEvent event) {
        this.paymentStatus = PaymentStatus.REFUNDED;
    }

    private void applyShipmentCreated(ShipmentCreatedEvent event) {
        this.status = OrderStatus.SHIPPED;
        this.shipmentStatus = ShipmentStatus.CREATED;
        this.shipmentId = event.shipmentId();
        this.trackingNumber = event.trackingNumber();
    }

    private void applyShipmentDelivered(ShipmentDeliveredEvent event) {
        this.status = OrderStatus.DELIVERED;
        this.shipmentStatus = ShipmentStatus.DELIVERED;
    }

    private void applyOrderCancelled(OrderCancelledEvent event) {
        this.status = OrderStatus.CANCELLED;
    }

    // ═══════════════════════════════════════════════════════════════════
    // RECONSTITUTION — Rebuild from events (Replay)
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Rebuilds an Order aggregate from a list of stored events.
     *
     * REPLAY EXPLAINED:
     * Instead of loading "current state" from a table, we replay ALL events
     * from the beginning of time. Each event is applied in sequence.
     * The final state is identical to what you'd get from a traditional DB.
     *
     * Benefits:
     *   - Complete audit trail
     *   - Can replay to any point in time for debugging
     *   - Can build new projections from old events
     *
     * Performance:
     *   - With 50+ events, we first load the latest SNAPSHOT
     *   - Then replay only events AFTER the snapshot
     *   - This bounds replay time to at most ~50 events
     *
     * @param events ordered list of events from version 1 onwards (or since snapshot)
     */
    public static Order reconstitute(List<DomainEvent> events) {
        if (events.isEmpty()) {
            throw new DomainException("Cannot reconstitute an Order from empty event list");
        }

        Order order = new Order();
        for (DomainEvent event : events) {
            order.apply(event);
        }
        return order;
    }

    /**
     * Rebuilds from a snapshot + subsequent events.
     * This is the performance optimization path used when an aggregate
     * has accumulated more than SNAPSHOT_THRESHOLD (50) events.
     */
    public static Order reconstituteFromSnapshot(OrderSnapshot snapshot, List<DomainEvent> eventsAfterSnapshot) {
        Order order = snapshot.toOrder();
        for (DomainEvent event : eventsAfterSnapshot) {
            order.apply(event);
        }
        return order;
    }

    // ═══════════════════════════════════════════════════════════════════
    // SNAPSHOT SUPPORT
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Creates a snapshot of current state for performance optimization.
     * Called after every SNAPSHOT_THRESHOLD events.
     *
     * The snapshot is persisted separately in the order_snapshots table.
     * On future loads, we load the snapshot + events AFTER the snapshot version.
     */
    public OrderSnapshot toSnapshot() {
        return OrderSnapshot.of(this);
    }

    /**
     * Restores an Order from a snapshot without going through event replay.
     * Package-private — only OrderSnapshot should call this.
     * Keeps the restoration logic co-located with the snapshot type.
     */
    static Order restoreFromSnapshot(OrderSnapshot snapshot) {
        Order order = new Order();
        order.id = OrderId.of(snapshot.aggregateId());
        order.version = snapshot.version();
        order.customerId = CustomerId.of(snapshot.customerId());
        order.status = snapshot.status();
        order.items = new ArrayList<>(snapshot.items() != null ? snapshot.items() : List.of());
        order.totalAmount = snapshot.totalAmount() != null ? snapshot.totalAmount() : Money.ZERO;
        order.paymentStatus = snapshot.paymentStatus() != null ? snapshot.paymentStatus() : PaymentStatus.PENDING;
        order.shipmentStatus = snapshot.shipmentStatus() != null ? snapshot.shipmentStatus() : ShipmentStatus.NOT_CREATED;
        order.shippingAddress = snapshot.shippingAddress();
        order.workflowId = snapshot.workflowId();
        order.reservationId = snapshot.reservationId();
        order.transactionId = snapshot.transactionId();
        order.shipmentId = snapshot.shipmentId();
        order.trackingNumber = snapshot.trackingNumber();
        return order;
    }

    // ═══════════════════════════════════════════════════════════════════
    // INFRASTRUCTURE METHODS
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Records and immediately applies an event.
     * "Apply first, persist later" pattern ensures the aggregate is
     * in the correct state for subsequent commands in the same request.
     */
    private void applyAndRecord(DomainEvent event) {
        apply(event);
        pendingEvents.add(event);
    }

    /**
     * Returns pending events and clears the list.
     * Called by the application service to drain events before persisting.
     * This is a "once per request" operation.
     */
    public List<DomainEvent> drainPendingEvents() {
        List<DomainEvent> events = List.copyOf(pendingEvents);
        pendingEvents.clear();
        return events;
    }

    /**
     * Validates that the current version matches an expected version.
     * Used for optimistic locking checks.
     *
     * OPTIMISTIC LOCKING FLOW:
     *   1. Load order at version N
     *   2. Handle command, raise events
     *   3. When saving events, WHERE aggregate_id=X AND version=N
     *   4. If 0 rows updated → another request modified it → throw exception
     */
    public void validateVersion(long expectedVersion) {
        if (this.version != expectedVersion) {
            throw new OptimisticLockingException(
                    "Order " + id + ": expected version " + expectedVersion +
                    " but current version is " + this.version);
        }
    }

    // ─────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────

    private Money calculateTotal() {
        return items.stream()
                .map(OrderItem::totalPrice)
                .reduce(Money.ZERO, Money::add);
    }

    private void requireStatus(OrderStatus required, String action) {
        if (this.status != required) {
            throw new InvalidStateTransitionException(
                    "Cannot " + action + " order in status " + this.status +
                    ". Required: " + required);
        }
    }

    // ─────────────────────────────────────────────────────
    // Getters — read-only access, no setters
    // ─────────────────────────────────────────────────────

    public OrderId getId() { return id; }
    public long getVersion() { return version; }
    public CustomerId getCustomerId() { return customerId; }
    public OrderStatus getStatus() { return status; }
    public List<OrderItem> getItems() { return Collections.unmodifiableList(items); }
    public Money getTotalAmount() { return totalAmount; }
    public PaymentStatus getPaymentStatus() { return paymentStatus; }
    public ShipmentStatus getShipmentStatus() { return shipmentStatus; }
    public String getShippingAddress() { return shippingAddress; }
    public String getWorkflowId() { return workflowId; }
    public String getReservationId() { return reservationId; }
    public String getTransactionId() { return transactionId; }
    public String getShipmentId() { return shipmentId; }
    public String getTrackingNumber() { return trackingNumber; }
    public boolean hasPendingEvents() { return !pendingEvents.isEmpty(); }
}
