package com.example.ordermanagement.domain.port.outbound;

import com.example.ordermanagement.domain.aggregate.OrderSnapshot;

import java.util.Optional;

/**
 * Output Port: SnapshotStore
 *
 * Manages snapshots of aggregate state for performance optimization.
 * Separate from the EventStore because snapshots are a technical optimization,
 * not a domain concept — they're expendable and can be regenerated from events.
 */
public interface SnapshotStore {

    /**
     * Saves a snapshot of aggregate state.
     * Overwrites any existing snapshot for this aggregate
     * (we only ever need the latest snapshot).
     */
    void saveSnapshot(OrderSnapshot snapshot);

    /**
     * Loads the latest snapshot for an aggregate.
     * Returns empty if no snapshot exists yet.
     */
    Optional<OrderSnapshot> loadLatestSnapshot(String aggregateId);
}
