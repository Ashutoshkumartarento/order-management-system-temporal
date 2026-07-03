package com.example.inventory.domain.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

public class Reservation implements Persistable<String> {

    @Id
    private final String reservationId;
    private final String orderId;
    private final String status;
    private final String itemsJson;   // JSON array of {productId, quantity}
    private final Instant createdAt;
    private final Instant updatedAt;

    @Transient
    private final boolean isNew;

    @PersistenceCreator
    public Reservation(String reservationId, String orderId, String status,
                       String itemsJson, Instant createdAt, Instant updatedAt) {
        this(reservationId, orderId, status, itemsJson, createdAt, updatedAt, false);
    }

    public Reservation(String reservationId, String orderId, String status,
                       String itemsJson, Instant createdAt, Instant updatedAt, boolean isNew) {
        this.reservationId = reservationId;
        this.orderId = orderId;
        this.status = status;
        this.itemsJson = itemsJson;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.isNew = isNew;
    }

    public static Reservation create(String orderId, String itemsJson) {
        String id = "INV-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        Instant now = Instant.now();
        return new Reservation(id, orderId, "ACTIVE", itemsJson, now, now, true);
    }

    public Reservation release() {
        return new Reservation(reservationId, orderId, "RELEASED", itemsJson,
                createdAt, Instant.now(), false);
    }

    @Override public String getId()    { return reservationId; }
    @Override public boolean isNew()   { return isNew; }

    public String  reservationId() { return reservationId; }
    public String  orderId()       { return orderId; }
    public String  status()        { return status; }
    public String  itemsJson()     { return itemsJson; }
    public Instant createdAt()     { return createdAt; }
    public Instant updatedAt()     { return updatedAt; }
}
