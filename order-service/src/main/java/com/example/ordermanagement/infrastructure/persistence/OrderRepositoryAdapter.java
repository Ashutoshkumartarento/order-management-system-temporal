package com.example.ordermanagement.infrastructure.persistence;

import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.aggregate.OrderSnapshot;
import com.example.ordermanagement.domain.event.DomainEvent;
import com.example.ordermanagement.domain.port.outbound.EventStore;
import com.example.ordermanagement.domain.port.outbound.OrderRepository;
import com.example.ordermanagement.domain.port.outbound.SnapshotStore;
import com.example.ordermanagement.domain.valueobject.OrderId;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * OrderRepositoryAdapter
 *
 * CHANGE FROM MONOLITH:
 * Added ApplicationEventPublisher to publish domain events as Spring
 * application events AFTER they are persisted to the event store.
 *
 * The OrderEventKafkaPublisher listens with @TransactionalEventListener(AFTER_COMMIT)
 * and publishes to Kafka only after the DB transaction commits.
 *
 * This implements the OUTBOX PATTERN without a separate outbox table:
 *   persist → spring event → AFTER_COMMIT → kafka publish
 */
@Repository
public class OrderRepositoryAdapter implements OrderRepository {

    private static final String AGGREGATE_TYPE = "Order";

    private final EventStore eventStore;
    private final SnapshotStore snapshotStore;
    private final ApplicationEventPublisher applicationEventPublisher;

    public OrderRepositoryAdapter(EventStore eventStore, SnapshotStore snapshotStore,
                                 ApplicationEventPublisher applicationEventPublisher) {
        this.eventStore = eventStore;
        this.snapshotStore = snapshotStore;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void save(Order order) {
        List<DomainEvent> pendingEvents = order.drainPendingEvents();
        if (pendingEvents.isEmpty()) return;

        long expectedVersion = order.getVersion() - pendingEvents.size();

        eventStore.appendEvents(order.getId().toString(), AGGREGATE_TYPE,
                pendingEvents, expectedVersion);

        // Snapshot check
        if (order.getVersion() % OrderSnapshot.SNAPSHOT_THRESHOLD == 0) {
            snapshotStore.saveSnapshot(order.toSnapshot());
        }

        // Publish each event as a Spring application event.
        // OrderEventKafkaPublisher picks these up AFTER_COMMIT.
        pendingEvents.forEach(applicationEventPublisher::publishEvent);
    }

    @Override
    public Optional<Order> findById(OrderId orderId) {
        String id = orderId.toString();
        if (!eventStore.exists(id)) return Optional.empty();

        var snapshotOpt = snapshotStore.loadLatestSnapshot(id);
        Order order;
        if (snapshotOpt.isPresent()) {
            var snapshot = snapshotOpt.get();
            List<DomainEvent> recent = eventStore.loadEvents(id, snapshot.version());
            order = Order.reconstituteFromSnapshot(snapshot, recent);
        } else {
            order = Order.reconstitute(eventStore.loadEvents(id));
        }
        return Optional.of(order);
    }
}
