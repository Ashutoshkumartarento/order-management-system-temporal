package com.example.ordermanagement.infrastructure.temporal.activity;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: InventoryActivity
 *
 * ═══════════════════════════════════════════════════════════════════
 * WHAT IS A TEMPORAL ACTIVITY?
 * ═══════════════════════════════════════════════════════════════════
 * Activities are the SIDE-EFFECT boundary in Temporal.
 * Everything that touches the outside world (DB, HTTP, queues) goes here.
 * Workflows are deterministic code; Activities are non-deterministic side effects.
 *
 * Activities can:
 *   - Call external HTTP APIs
 *   - Read/write databases
 *   - Send emails/notifications
 *   - Call domain services
 *
 * Activities CANNOT directly modify workflow state — they return results
 * which the workflow uses to make decisions.
 *
 * ═══════════════════════════════════════════════════════════════════
 * ACTIVITY METHODS
 * ═══════════════════════════════════════════════════════════════════
 * Methods here fall into two categories:
 *
 * 1. ACTION methods: call external systems (reserveInventory, releaseInventory)
 *    These simulate calling an Inventory microservice.
 *
 * 2. RECORD methods: call the domain service to persist events (recordInventoryReserved)
 *    These bridge Temporal back to the event store.
 *    They use idempotency keys to avoid duplicate events on retry.
 */
@ActivityInterface
public interface InventoryActivity {

    /**
     * Calls the inventory service to reserve stock.
     * May throw if inventory is unavailable (simulated via failure rate config).
     */
    @ActivityMethod
    ReservationResult reserveInventory(String orderId);

    /**
     * Releases previously reserved inventory.
     * This is the COMPENSATION action for the saga.
     */
    @ActivityMethod
    void releaseInventory(String orderId, String reservationId);

    /**
     * Records InventoryReservedEvent in the domain event store.
     * Idempotent: safe to call multiple times with same reservationId.
     */
    @ActivityMethod
    void recordInventoryReserved(String orderId, String reservationId);

    /**
     * Records InventoryReservationFailedEvent.
     */
    @ActivityMethod
    void recordInventoryReservationFailed(String orderId, String reason);

    /**
     * Records InventoryReleasedEvent.
     */
    @ActivityMethod
    void recordInventoryReleased(String orderId, String reason);

    /**
     * Records OrderCancelledEvent (used by multiple compensation paths).
     */
    @ActivityMethod
    void recordOrderCancelled(String orderId, String reason, String cancelledBy);

    record ReservationResult(String reservationId, String message) {}
}
