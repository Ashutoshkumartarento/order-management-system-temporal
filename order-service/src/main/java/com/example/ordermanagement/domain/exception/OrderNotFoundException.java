package com.example.ordermanagement.domain.exception;

/**
 * Thrown when an order cannot be found by its ID.
 * Maps to HTTP 404 Not Found.
 */
public class OrderNotFoundException extends DomainException {
    public OrderNotFoundException(String orderId) {
        super("Order not found: " + orderId);
    }
}
