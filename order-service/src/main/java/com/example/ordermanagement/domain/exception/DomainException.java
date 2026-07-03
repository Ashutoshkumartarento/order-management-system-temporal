package com.example.ordermanagement.domain.exception;

/**
 * Base domain exception.
 * All business rule violations extend this class.
 * This allows the API layer to catch domain errors uniformly
 * and translate them to appropriate HTTP responses (typically 400 or 409).
 */
public class DomainException extends RuntimeException {
    public DomainException(String message) {
        super(message);
    }
    public DomainException(String message, Throwable cause) {
        super(message, cause);
    }
}
