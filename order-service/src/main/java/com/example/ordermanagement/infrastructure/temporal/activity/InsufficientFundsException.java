package com.example.ordermanagement.infrastructure.temporal.activity;

/**
 * Non-retryable exception: payment failed due to insufficient funds.
 * Temporal will NOT retry activities that throw this.
 */
public class InsufficientFundsException extends RuntimeException {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(InsufficientFundsException.class);
    public InsufficientFundsException(String message) {
        super(message);
    }
}
