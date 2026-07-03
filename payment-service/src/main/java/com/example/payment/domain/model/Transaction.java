package com.example.payment.domain.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class Transaction implements Persistable<String> {

    @Id
    private final String transactionId;
    private final String orderId;
    private final BigDecimal amount;
    private final String currency;
    private final String type;
    private final String status;
    private final String failureReason;
    private final Instant createdAt;

    @Transient
    private final boolean isNew;

    @PersistenceCreator
    public Transaction(String transactionId, String orderId, BigDecimal amount,
                       String currency, String type, String status,
                       String failureReason, Instant createdAt) {
        this(transactionId, orderId, amount, currency, type, status, failureReason, createdAt, false);
    }

    public Transaction(String transactionId, String orderId, BigDecimal amount,
                       String currency, String type, String status,
                       String failureReason, Instant createdAt, boolean isNew) {
        this.transactionId = transactionId;
        this.orderId = orderId;
        this.amount = amount;
        this.currency = currency;
        this.type = type;
        this.status = status;
        this.failureReason = failureReason;
        this.createdAt = createdAt;
        this.isNew = isNew;
    }

    public static Transaction charge(String orderId, BigDecimal amount, String currency) {
        String id = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return new Transaction(id, orderId, amount, currency, "CHARGE", "COMPLETED", null, Instant.now(), true);
    }

    public static Transaction refund(String refundForTransactionId, String orderId, BigDecimal amount) {
        String id = "REFUND-" + refundForTransactionId;
        return new Transaction(id, orderId, amount, "USD", "REFUND", "COMPLETED", null, Instant.now(), true);
    }

    public Transaction fail(String reason) {
        return new Transaction(transactionId, orderId, amount, currency, type, "FAILED", reason, createdAt, false);
    }

    @Override public String getId() { return transactionId; }
    @Override public boolean isNew() { return isNew; }

    public String transactionId()  { return transactionId; }
    public String orderId()        { return orderId; }
    public BigDecimal amount()     { return amount; }
    public String currency()       { return currency; }
    public String type()           { return type; }
    public String status()         { return status; }
    public String failureReason()  { return failureReason; }
    public Instant createdAt()     { return createdAt; }
}
