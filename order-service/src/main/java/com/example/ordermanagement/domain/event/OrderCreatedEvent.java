package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.CustomerId;
import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: OrderCreatedEvent
 *
 * Raised when a new order is created.
 * This is the "genesis event" for every Order aggregate.
 * Without this event, the aggregate cannot be replayed into existence.
 *
 * Why record? Records in Java 21 are implicitly:
 * - Immutable (no setters)
 * - Have equals/hashCode based on fields
 * - Have a compact toString
 * Perfect for events which are immutable facts.
 *
 * The @JsonCreator + @JsonProperty annotations allow Jackson to deserialize
 * this record correctly during event replay from the event store.
 */
public record OrderCreatedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        CustomerId customerId,
        String shippingAddress
) implements DomainEvent {

    @JsonCreator
    public static OrderCreatedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("customerId") CustomerId customerId,
            @JsonProperty("shippingAddress") String shippingAddress) {
        return new OrderCreatedEvent(eventId, aggregateId, version, occurredAt, customerId, shippingAddress);
    }

    public static OrderCreatedEvent create(OrderId orderId, CustomerId customerId, String shippingAddress, long version) {
        return new OrderCreatedEvent(
                UUID.randomUUID(),
                orderId.toString(),
                version,
                Instant.now(),
                customerId,
                shippingAddress
        );
    }
}
