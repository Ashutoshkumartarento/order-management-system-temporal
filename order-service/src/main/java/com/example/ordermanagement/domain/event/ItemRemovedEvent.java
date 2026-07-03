package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: ItemRemovedEvent
 *
 * Raised when an item is removed from a DRAFT order.
 * We only store the productId (not the full item) — the aggregate
 * uses this to locate and remove the item from its list.
 */
public record ItemRemovedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        UUID productId
) implements DomainEvent {

    @JsonCreator
    public static ItemRemovedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("productId") UUID productId) {
        return new ItemRemovedEvent(eventId, aggregateId, version, occurredAt, productId);
    }

    public static ItemRemovedEvent create(OrderId orderId, UUID productId, long version) {
        return new ItemRemovedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), productId);
    }
}
