package com.example.inventory.domain.model;

/** Thrown when a reserve() request exceeds available stock. Non-retryable. */
public class InsufficientStockException extends RuntimeException {
    public InsufficientStockException(String message) {
        super(message);
    }
}