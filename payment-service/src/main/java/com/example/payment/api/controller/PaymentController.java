package com.example.payment.api.controller;

import com.example.contracts.api.PaymentContracts;
import com.example.payment.application.service.PaymentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Tag(name = "Payment", description = "Charge and refund operations")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @Operation(summary = "Charge payment for an order")
    @PostMapping("/charge")
    public ResponseEntity<PaymentContracts.ChargePaymentResponse> charge(
            @Valid @RequestBody PaymentContracts.ChargePaymentRequest request) {
        return ResponseEntity.ok(paymentService.charge(request));
    }

    @Operation(summary = "Refund a payment (saga compensation)")
    @PostMapping("/refund")
    public ResponseEntity<PaymentContracts.RefundPaymentResponse> refund(
            @Valid @RequestBody PaymentContracts.RefundPaymentRequest request) {
        return ResponseEntity.ok(paymentService.refund(request));
    }

    @ExceptionHandler({PaymentService.InsufficientFundsException.class,
                       PaymentService.CardDeclinedException.class})
    public ResponseEntity<String> handleNonRetryable(RuntimeException ex) {
        // HTTP 422 signals non-retryable to PaymentActivityImpl
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(ex.getMessage());
    }
}
