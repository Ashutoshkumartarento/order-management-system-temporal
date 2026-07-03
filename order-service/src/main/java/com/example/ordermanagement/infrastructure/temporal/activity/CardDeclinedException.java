package com.example.ordermanagement.infrastructure.temporal.activity;

/**
 * Non-retryable exception: card was declined by the issuing bank.
 * Temporal will NOT retry activities that throw this.
 */
public class CardDeclinedException extends RuntimeException {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CardDeclinedException.class);
    public CardDeclinedException(String message) {
        super(message);
    }
}
