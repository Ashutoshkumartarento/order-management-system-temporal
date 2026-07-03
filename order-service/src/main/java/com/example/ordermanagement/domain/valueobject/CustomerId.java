package com.example.ordermanagement.domain.valueobject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.UUID;

/**
 * Value Object: CustomerId
 *
 * Represents the identity of a customer in the order domain.
 * In a real microservices environment, this would be the ID used to call
 * the Customer bounded context for customer details.
 */
public record CustomerId(UUID value) {

    public CustomerId {
        Objects.requireNonNull(value, "CustomerId value must not be null");
    }

    public static CustomerId generate() {
        return new CustomerId(UUID.randomUUID());
    }

    @JsonCreator
    public static CustomerId of(String value) {
        return new CustomerId(UUID.fromString(value));
    }

    public static CustomerId of(UUID value) {
        return new CustomerId(value);
    }

    @JsonValue
    @Override
    public String toString() {
        return value.toString();
    }
}
