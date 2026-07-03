package com.example.payment.domain.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class Refund implements Persistable<String> {

    @Id
    private final String refundId;
    private final String orderId;
    private final String originalTransactionId;
    private final BigDecimal amount;
    private final String currency;
    private final String status;
    private final Instant createdAt;

    @Transient
    private final boolean isNew;

    @PersistenceCreator
    public Refund(String refundId, String orderId, String originalTransactionId,
                  BigDecimal amount, String currency, String status,
                  Instant createdAt) {
        this(refundId, orderId, originalTransactionId, amount, currency, status, createdAt, false);
    }

    public Refund(String refundId, String orderId, String originalTransactionId,
                  BigDecimal amount, String currency, String status,
                  Instant createdAt, boolean isNew) {
        this.refundId               = refundId;
        this.orderId                = orderId;
        this.originalTransactionId  = originalTransactionId;
        this.amount                 = amount;
        this.currency               = currency;
        this.status                 = status;
        this.createdAt              = createdAt;
        this.isNew                  = isNew;
    }

    public static Refund create(String orderId, String originalTransactionId,
                                BigDecimal amount, String currency) {
        String id = "REF-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return new Refund(id, orderId, originalTransactionId,
                amount, currency, "COMPLETED", Instant.now(), true);
    }

    @Override public String  getId()   { return refundId; }
    @Override public boolean isNew()   { return isNew; }

    public String     refundId()               { return refundId; }
    public String     orderId()                { return orderId; }
    public String     originalTransactionId()  { return originalTransactionId; }
    public BigDecimal amount()                 { return amount; }
    public String     currency()               { return currency; }
    public String     status()                 { return status; }
    public Instant    createdAt()              { return createdAt; }
}