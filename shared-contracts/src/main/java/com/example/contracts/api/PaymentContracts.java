package com.example.contracts.api;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;

import java.math.BigDecimal;

/**
 * HTTP API contracts for the Payment Service.
 */
public final class PaymentContracts {

    private PaymentContracts() {}

    public record ChargePaymentRequest(
            @NotBlank String orderId,
            @DecimalMin("0.01") BigDecimal amount,
            String currency
    ) {}

    public record ChargePaymentResponse(
            String transactionId,
            String status,      // "CHARGED"
            String message
    ) {}

    public record RefundPaymentRequest(
            @NotBlank String orderId,
            @NotBlank String transactionId
    ) {}

    public record RefundPaymentResponse(
            String refundTransactionId,
            String status       // "REFUNDED"
    ) {}
}
