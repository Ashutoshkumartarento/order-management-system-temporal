package com.example.ordermanagement.domain.model;

/**
 * Enum: PaymentStatus
 *
 * Tracks the payment sub-status independently from the overall order status.
 * This separation allows querying payment state without interpreting the
 * composite OrderStatus, and supports the event sourcing replay model cleanly.
 */
public enum PaymentStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    REFUNDED
}
