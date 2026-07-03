package com.example.ordermanagement.infrastructure.persistence;

import com.example.ordermanagement.config.JacksonConfig;
import com.example.ordermanagement.domain.event.*;
import com.example.ordermanagement.domain.exception.OptimisticLockingException;
import com.example.ordermanagement.domain.valueobject.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration Tests: EventStoreAdapter
 *
 * Tests the event store with a REAL PostgreSQL database via Testcontainers.
 * This is more valuable than mocking because we're testing:
 * - Actual SQL execution
 * - Flyway migration correctness
 * - JSON serialization/deserialization round-trips
 * - Optimistic locking constraint behavior
 *
 * TESTCONTAINERS:
 * Starts a fresh PostgreSQL container per test class.
 * Tests run in isolation with a clean database.
 * Docker must be running for these tests.
 *
 * These tests run slower than unit tests (~5s per class for container startup)
 * but provide high confidence in the persistence layer.
 */
@Tag("integration")
@Testcontainers
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({EventStoreAdapter.class, JacksonConfig.class})
@DisplayName("EventStore Integration Tests")
class EventStoreIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("orderdb_test")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.enabled", () -> "true");
    }

    @Autowired
    EventStoreAdapter eventStore;

    @Test
    @DisplayName("Should append and load events correctly")
    void shouldAppendAndLoadEvents() {
        String aggregateId = UUID.randomUUID().toString();
        OrderId orderId = OrderId.of(aggregateId);
        CustomerId customerId = CustomerId.generate();

        List<DomainEvent> events = List.of(
                OrderCreatedEvent.create(orderId, customerId, "Test Address", 1L),
                ItemAddedEvent.create(orderId,
                        new OrderItem(UUID.randomUUID(), "Widget", 2, Money.of(10.00)), 2L)
        );

        eventStore.appendEvents(aggregateId, "Order", events, 0L);

        List<DomainEvent> loaded = eventStore.loadEvents(aggregateId);

        assertThat(loaded).hasSize(2);
        assertThat(loaded.get(0)).isInstanceOf(OrderCreatedEvent.class);
        assertThat(loaded.get(1)).isInstanceOf(ItemAddedEvent.class);
    }

    @Test
    @DisplayName("Should enforce optimistic locking")
    void shouldEnforceOptimisticLocking() {
        String aggregateId = UUID.randomUUID().toString();
        OrderId orderId = OrderId.of(aggregateId);
        CustomerId customerId = CustomerId.generate();

        // First append succeeds
        List<DomainEvent> firstEvents = List.of(
                OrderCreatedEvent.create(orderId, customerId, "Address", 1L)
        );
        eventStore.appendEvents(aggregateId, "Order", firstEvents, 0L);

        // Second append with wrong expected version fails
        List<DomainEvent> secondEvents = List.of(
                ItemAddedEvent.create(orderId,
                        new OrderItem(UUID.randomUUID(), "Widget", 1, Money.of(5.00)), 2L)
        );

        // Simulate concurrent modification: another request already wrote version 2
        // We expect version 1 but actual is 1 (correct), so first concurrent write wins
        List<DomainEvent> conflictingEvents = List.of(
                ItemAddedEvent.create(orderId,
                        new OrderItem(UUID.randomUUID(), "Conflicting Item", 1, Money.of(5.00)), 2L)
        );

        // First write at version 1 → succeeds
        eventStore.appendEvents(aggregateId, "Order", secondEvents, 1L);

        // Second write also claims expected version = 1, but current is now 2 → fails
        assertThatThrownBy(() ->
                eventStore.appendEvents(aggregateId, "Order", conflictingEvents, 1L))
                .isInstanceOf(OptimisticLockingException.class);
    }

    @Test
    @DisplayName("Should load events from specific version")
    void shouldLoadEventsFromVersion() {
        String aggregateId = UUID.randomUUID().toString();
        OrderId orderId = OrderId.of(aggregateId);
        CustomerId customerId = CustomerId.generate();

        // Append 3 events
        eventStore.appendEvents(aggregateId, "Order", List.of(
                OrderCreatedEvent.create(orderId, customerId, "Address", 1L)
        ), 0L);

        eventStore.appendEvents(aggregateId, "Order", List.of(
                ItemAddedEvent.create(orderId, new OrderItem(UUID.randomUUID(), "A", 1, Money.of(5.00)), 2L)
        ), 1L);

        eventStore.appendEvents(aggregateId, "Order", List.of(
                OrderConfirmedEvent.create(orderId, Money.of(5.00), "wf-1", 3L)
        ), 2L);

        // Load only events after version 1 (simulating post-snapshot load)
        List<DomainEvent> eventsAfterV1 = eventStore.loadEvents(aggregateId, 1L);
        assertThat(eventsAfterV1).hasSize(2);
        assertThat(eventsAfterV1.get(0)).isInstanceOf(ItemAddedEvent.class);
        assertThat(eventsAfterV1.get(1)).isInstanceOf(OrderConfirmedEvent.class);
    }

    @Test
    @DisplayName("Should correctly serialize and deserialize all event types")
    void shouldSerializeAndDeserializeAllEventTypes() {
        String aggregateId = UUID.randomUUID().toString();
        OrderId orderId = OrderId.of(aggregateId);
        CustomerId customerId = CustomerId.generate();
        UUID productId = UUID.randomUUID();

        // Build a complete lifecycle
        List<DomainEvent> lifecycle = List.of(
                OrderCreatedEvent.create(orderId, customerId, "123 Test St", 1L),
                ItemAddedEvent.create(orderId, new OrderItem(productId, "Widget", 2, Money.of(10.00)), 2L),
                OrderConfirmedEvent.create(orderId, Money.of(20.00), "wf-serial", 3L),
                InventoryReservedEvent.create(orderId, "RES-SERIAL", 4L),
                PaymentCompletedEvent.create(orderId, "TXN-SERIAL", Money.of(20.00), 5L),
                ShipmentCreatedEvent.create(orderId, "SHIP-SERIAL", "TRACK-SERIAL", "UPS", 6L),
                ShipmentDeliveredEvent.create(orderId, "SHIP-SERIAL", 7L)
        );

        // Append each event
        for (int i = 0; i < lifecycle.size(); i++) {
            eventStore.appendEvents(aggregateId, "Order", List.of(lifecycle.get(i)), (long) i);
        }

        // Load and verify
        List<DomainEvent> loaded = eventStore.loadEvents(aggregateId);
        assertThat(loaded).hasSize(lifecycle.size());

        // Verify type preservation
        assertThat(loaded.get(0)).isInstanceOf(OrderCreatedEvent.class);
        assertThat(loaded.get(1)).isInstanceOf(ItemAddedEvent.class);
        assertThat(loaded.get(2)).isInstanceOf(OrderConfirmedEvent.class);
        assertThat(loaded.get(3)).isInstanceOf(InventoryReservedEvent.class);
        assertThat(loaded.get(4)).isInstanceOf(PaymentCompletedEvent.class);
        assertThat(loaded.get(5)).isInstanceOf(ShipmentCreatedEvent.class);
        assertThat(loaded.get(6)).isInstanceOf(ShipmentDeliveredEvent.class);
    }

    @Test
    @DisplayName("Should replay events to reconstruct order state")
    void shouldReplayEventsToReconstructOrder() {
        String aggregateId = UUID.randomUUID().toString();
        OrderId orderId = OrderId.of(aggregateId);
        CustomerId customerId = CustomerId.generate();

        // Create order through lifecycle and persist events
        com.example.ordermanagement.domain.aggregate.Order order =
                com.example.ordermanagement.domain.aggregate.Order.create(orderId, customerId, "Replay Test Address");
        order.addItem(new OrderItem(UUID.randomUUID(), "Widget", 3, Money.of(25.00)));
        order.confirm("wf-replay-test");
        order.reserveInventory("RES-REPLAY-TEST");

        List<DomainEvent> events = order.drainPendingEvents();
        eventStore.appendEvents(aggregateId, "Order", events, 0L);

        // Load events and reconstitute — this is the replay
        List<DomainEvent> loadedEvents = eventStore.loadEvents(aggregateId);
        com.example.ordermanagement.domain.aggregate.Order reconstructed =
                com.example.ordermanagement.domain.aggregate.Order.reconstitute(loadedEvents);

        assertThat(reconstructed.getStatus())
                .isEqualTo(com.example.ordermanagement.domain.model.OrderStatus.INVENTORY_RESERVED);
        assertThat(reconstructed.getTotalAmount()).isEqualTo(Money.of(75.00));
        assertThat(reconstructed.getReservationId()).isEqualTo("RES-REPLAY-TEST");
    }
}
