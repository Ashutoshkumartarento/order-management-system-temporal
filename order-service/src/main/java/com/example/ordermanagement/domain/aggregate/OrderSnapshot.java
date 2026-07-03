package com.example.ordermanagement.domain.aggregate;

import com.example.ordermanagement.domain.model.OrderStatus;
import com.example.ordermanagement.domain.model.PaymentStatus;
import com.example.ordermanagement.domain.model.ShipmentStatus;
import com.example.ordermanagement.domain.valueobject.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * OrderSnapshot — Aggregate state at a specific version
 *
 * ═══════════════════════════════════════════════════════════════════
 * WHY SNAPSHOTS?
 * ═══════════════════════════════════════════════════════════════════
 * Event Sourcing requires replaying all events to rebuild state.
 * For an order with 3 events: fast.
 * For an order with 500 events (customer changed items, multiple retries): slow.
 *
 * Snapshotting solves this:
 *   - Every SNAPSHOT_THRESHOLD (50) events, save current state as snapshot
 *   - Future loads: load snapshot at version N + replay events from N+1 onwards
 *   - Bounded replay: at most 50 events need replaying
 *
 * SCHEMA: order_snapshots table
 *   - aggregate_id
 *   - version (the version at snapshot time)
 *   - snapshot_data (JSON of this record)
 *   - created_at
 *
 * WHEN IS A SNAPSHOT TAKEN?
 * After the application service persists events, it checks:
 *   if (order.getVersion() % SNAPSHOT_THRESHOLD == 0) { takeSnapshot(); }
 *
 * The snapshot captures EXACTLY the state after applying the event at that version.
 *
 * SNAPSHOT EVOLUTION:
 * Old snapshots may have missing fields as the domain evolves.
 * Use @JsonProperty with defaultValue or Optional to handle missing fields gracefully.
 * Never delete old snapshots — they must remain deserializable forever.
 */
public record OrderSnapshot(
        UUID snapshotId,
        String aggregateId,
        long version,
        Instant takenAt,
        String customerId,
        OrderStatus status,
        List<OrderItem> items,
        Money totalAmount,
        PaymentStatus paymentStatus,
        ShipmentStatus shipmentStatus,
        String shippingAddress,
        String workflowId,
        String reservationId,
        String transactionId,
        String shipmentId,
        String trackingNumber
) {

    public static final int SNAPSHOT_THRESHOLD = 50;

    @JsonCreator
    public static OrderSnapshot of(
            @JsonProperty("snapshotId") UUID snapshotId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("takenAt") Instant takenAt,
            @JsonProperty("customerId") String customerId,
            @JsonProperty("status") OrderStatus status,
            @JsonProperty("items") List<OrderItem> items,
            @JsonProperty("totalAmount") Money totalAmount,
            @JsonProperty("paymentStatus") PaymentStatus paymentStatus,
            @JsonProperty("shipmentStatus") ShipmentStatus shipmentStatus,
            @JsonProperty("shippingAddress") String shippingAddress,
            @JsonProperty("workflowId") String workflowId,
            @JsonProperty("reservationId") String reservationId,
            @JsonProperty("transactionId") String transactionId,
            @JsonProperty("shipmentId") String shipmentId,
            @JsonProperty("trackingNumber") String trackingNumber) {
        return new OrderSnapshot(snapshotId, aggregateId, version, takenAt, customerId,
                status, items, totalAmount, paymentStatus, shipmentStatus, shippingAddress,
                workflowId, reservationId, transactionId, shipmentId, trackingNumber);
    }

    /**
     * Creates a snapshot from an Order aggregate.
     * Called by the infrastructure layer when threshold is reached.
     */
    public static OrderSnapshot of(Order order) {
        return new OrderSnapshot(
                UUID.randomUUID(),
                order.getId().toString(),
                order.getVersion(),
                Instant.now(),
                order.getCustomerId().toString(),
                order.getStatus(),
                new ArrayList<>(order.getItems()),
                order.getTotalAmount(),
                order.getPaymentStatus(),
                order.getShipmentStatus(),
                order.getShippingAddress(),
                order.getWorkflowId(),
                order.getReservationId(),
                order.getTransactionId(),
                order.getShipmentId(),
                order.getTrackingNumber()
        );
    }

    /**
     * Reconstructs a mutable Order aggregate from this snapshot.
     * Used as the starting point for subsequent event replay.
     *
     * This uses reflection-free restoration via the domain events.
     * We use a "restore" method on Order to avoid exposing setters.
     */
    public Order toOrder() {
        return Order.restoreFromSnapshot(this);
    }
}
