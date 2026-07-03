package com.example.payment.application.service;

import com.example.contracts.api.PaymentContracts;
import com.example.contracts.kafka.PaymentEventMessage;
import com.example.payment.domain.model.Refund;
import com.example.payment.domain.model.Transaction;
import com.example.payment.infrastructure.persistence.RefundRepository;
import com.example.payment.infrastructure.persistence.TransactionRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class PaymentService {

    private final TransactionRepository    transactionRepository;
    private final RefundRepository         refundRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Value("${simulation.payment-failure-rate:0.0}")
    private double failureRate;

    @Value("${simulation.payment-transient-ratio:0.3}")
    private double transientRatio;

    public PaymentService(TransactionRepository transactionRepository,
                          RefundRepository refundRepository,
                          ApplicationEventPublisher eventPublisher) {
        this.transactionRepository = transactionRepository;
        this.refundRepository      = refundRepository;
        this.eventPublisher        = eventPublisher;
    }

    @Transactional
    public PaymentContracts.ChargePaymentResponse charge(
            PaymentContracts.ChargePaymentRequest request) {

        // Idempotency: return existing charge if already processed for this order
        var existing = transactionRepository.findFirstByOrderIdAndTypeOrderByCreatedAtDesc(
                request.orderId(), "CHARGE");
        if (existing.isPresent()) {
            return new PaymentContracts.ChargePaymentResponse(
                    existing.get().transactionId(), "CHARGED", "Already charged (idempotent)");
        }

        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            double failType = ThreadLocalRandom.current().nextDouble();
            if (failType < transientRatio) {
                throw new RuntimeException("Payment gateway timeout (transient)");
            } else if (failType < 0.7) {
                throw new InsufficientFundsException("INSUFFICIENT_FUNDS: account balance too low");
            } else {
                throw new CardDeclinedException("CARD_DECLINED: rejected by issuing bank");
            }
        }

        Transaction transaction = Transaction.charge(
                request.orderId(), request.amount(), request.currency());
        transactionRepository.save(transaction);

        eventPublisher.publishEvent(new PaymentEventMessage.PaymentChargedMessage(
                UUID.randomUUID(), request.orderId(),
                transaction.transactionId(), request.amount(), request.currency(), Instant.now()));

        return new PaymentContracts.ChargePaymentResponse(
                transaction.transactionId(), "CHARGED", "Payment successful");
    }

    @Transactional
    public PaymentContracts.RefundPaymentResponse refund(
            PaymentContracts.RefundPaymentRequest request) {

        // Idempotency: return existing refund if this charge was already refunded
        var existing = refundRepository.findByOriginalTransactionId(request.transactionId());
        if (existing.isPresent()) {
            return new PaymentContracts.RefundPaymentResponse(
                    existing.get().refundId(), "REFUNDED");
        }

        Transaction original = transactionRepository.findById(request.transactionId())
                .orElseThrow(() -> new RuntimeException(
                        "Original transaction not found: " + request.transactionId()));

        Refund refund = Refund.create(
                request.orderId(),
                request.transactionId(),
                original.amount(),
                original.currency());
        refundRepository.save(refund);

        eventPublisher.publishEvent(new PaymentEventMessage.PaymentRefundedMessage(
                UUID.randomUUID(), request.orderId(),
                request.transactionId(), refund.refundId(),
                original.amount(), Instant.now()));

        return new PaymentContracts.RefundPaymentResponse(refund.refundId(), "REFUNDED");
    }

    public static class InsufficientFundsException extends RuntimeException {
        public InsufficientFundsException(String message) { super(message); }
    }

    public static class CardDeclinedException extends RuntimeException {
        public CardDeclinedException(String message) { super(message); }
    }
}