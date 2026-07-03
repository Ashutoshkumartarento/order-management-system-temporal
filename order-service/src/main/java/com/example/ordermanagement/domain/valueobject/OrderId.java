package com.example.ordermanagement.domain.valueobject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.UUID;

/**
 * Value Object: OrderId
 *
 * Why a value object instead of a raw UUID?
 * - Type safety: you cannot accidentally pass a CustomerId where an OrderId is expected
 * - Self-documenting code: method signatures are unambiguous
 * - Encapsulates the ID generation strategy (UUID here, could be ULID in production)
 * - Jackson annotations allow transparent JSON serialization as a simple string
 *
 * DDD Principle: Value Objects are immutable and defined by their value, not identity.
 */
public record OrderId(UUID value) {

    public OrderId {
        Objects.requireNonNull(value, "OrderId value must not be null");
    }

    /**
     * Factory method for generating new OrderIds.
     * Centralizing generation here means if we ever switch from UUID to ULID,
     * only this class needs changing.
     */
    public static OrderId generate() {
        return new OrderId(UUID.randomUUID());
    }

    @JsonCreator
    public static OrderId of(String value) {
        return new OrderId(UUID.fromString(value));
    }

    public static OrderId of(UUID value) {
        return new OrderId(value);
    }

    @JsonValue
    @Override
    public String toString() {
        return value.toString();
    }
}
