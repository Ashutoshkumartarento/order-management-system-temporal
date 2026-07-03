package com.example.ordermanagement.infrastructure.temporal.activity;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: PaymentActivity
 *
 * Handles payment processing with support for:
 * - Simulated failures at configurable rates
 * - Non-retryable errors (card declined, insufficient funds)
 * - Compensation via refund
 */
@ActivityInterface
public interface PaymentActivity {

    /**
     * Processes payment for the order.
     * May throw InsufficientFundsException or CardDeclinedException
     * which are non-retryable.
     * May throw transient exceptions which Temporal will retry.
     */
    @ActivityMethod
    PaymentResult processPayment(String orderId);

    /**
     * Issues a refund for a previously completed payment.
     * This is the compensation action for the saga.
     */
    @ActivityMethod
    void refundPayment(String orderId, String transactionId);

    @ActivityMethod
    void recordPaymentCompleted(String orderId, String transactionId);

    @ActivityMethod
    void recordPaymentFailed(String orderId, String reason, boolean retryable);

    @ActivityMethod
    void recordRefundCompleted(String orderId, String refundTransactionId);

    record PaymentResult(String transactionId, String message) {}
}
