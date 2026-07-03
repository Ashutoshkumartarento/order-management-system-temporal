package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.example.ordermanagement.domain.valueobject.OrderItem;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: ItemAddedEvent
 *
 * Raised when a product item is added to an order in DRAFT state.
 * The full OrderItem (including price snapshot) is embedded in the event.
 *
 * WHY EMBED THE PRICE?
 * Product prices change over time. By capturing the price at the moment
 * of adding the item, we preserve historical accuracy. This is a key
 * advantage of event sourcing over traditional CRUD — you see EXACTLY
 * what the price was when the customer added the item.
 */
public record ItemAddedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        OrderItem item
) implements DomainEvent {

    @JsonCreator
    public static ItemAddedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("item") OrderItem item) {
        return new ItemAddedEvent(eventId, aggregateId, version, occurredAt, item);
    }

    public static ItemAddedEvent create(OrderId orderId, OrderItem item, long version) {
        return new ItemAddedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), item);
    }
}
