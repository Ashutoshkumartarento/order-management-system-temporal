package com.example.ordermanagement.domain.valueobject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

/**
 * Value Object: OrderItem
 *
 * Represents a single line item within an order.
 * Note it is a VALUE OBJECT, not an Entity — it has no independent lifecycle.
 * Two OrderItems with identical productId, quantity, and price are equivalent.
 *
 * This is stored as part of the event payload in the event store,
 * NOT as a separate table row — that would require JOIN and break
 * the event sourcing model.
 */
public record OrderItem(
        UUID productId,
        String productName,
        int quantity,
        Money unitPrice
) {

    public OrderItem {
        Objects.requireNonNull(productId, "productId must not be null");
        Objects.requireNonNull(productName, "productName must not be null");
        Objects.requireNonNull(unitPrice, "unitPrice must not be null");
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be > 0, got: " + quantity);
        }
        if (productName.isBlank()) {
            throw new IllegalArgumentException("productName must not be blank");
        }
    }

    @JsonCreator
    public static OrderItem of(
            @JsonProperty("productId") UUID productId,
            @JsonProperty("productName") String productName,
            @JsonProperty("quantity") int quantity,
            @JsonProperty("unitPrice") Money unitPrice) {
        return new OrderItem(productId, productName, quantity, unitPrice);
    }

    /** Calculates the total price for this line item */
    public Money totalPrice() {
        return unitPrice.multiply(quantity);
    }

    /** Returns a new OrderItem with updated quantity — immutable update pattern */
    public OrderItem withQuantity(int newQuantity) {
        return new OrderItem(productId, productName, newQuantity, unitPrice);
    }
}
