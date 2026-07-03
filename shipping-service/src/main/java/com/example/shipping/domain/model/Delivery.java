package com.example.shipping.domain.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

public class Delivery implements Persistable<String> {

    @Id
    private final String deliveryId;
    private final String shipmentId;
    private final String orderId;
    private final Instant confirmedAt;

    @Transient
    private final boolean isNew;

    @PersistenceCreator
    public Delivery(String deliveryId, String shipmentId, String orderId,
                    Instant confirmedAt) {
        this(deliveryId, shipmentId, orderId, confirmedAt, false);
    }

    public Delivery(String deliveryId, String shipmentId, String orderId,
                    Instant confirmedAt, boolean isNew) {
        this.deliveryId  = deliveryId;
        this.shipmentId  = shipmentId;
        this.orderId     = orderId;
        this.confirmedAt = confirmedAt;
        this.isNew       = isNew;
    }

    public static Delivery create(String shipmentId, String orderId) {
        String id = "DEL-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return new Delivery(id, shipmentId, orderId, Instant.now(), true);
    }

    @Override public String  getId()  { return deliveryId; }
    @Override public boolean isNew()  { return isNew; }

    public String  deliveryId()  { return deliveryId; }
    public String  shipmentId()  { return shipmentId; }
    public String  orderId()     { return orderId; }
    public Instant confirmedAt() { return confirmedAt; }
}