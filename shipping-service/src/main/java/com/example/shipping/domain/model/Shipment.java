package com.example.shipping.domain.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

public class Shipment implements Persistable<String> {

    @Id
    private final String shipmentId;
    private final String orderId;
    private final String trackingNumber;
    private final String carrier;
    private final String status;
    private final Instant createdAt;
    private final Instant deliveredAt;

    @Transient
    private final boolean isNew;

    @PersistenceCreator
    public Shipment(String shipmentId, String orderId, String trackingNumber,
                    String carrier, String status, Instant createdAt,
                    Instant deliveredAt) {
        this(shipmentId, orderId, trackingNumber, carrier, status, createdAt, deliveredAt, false);
    }

    public Shipment(String shipmentId, String orderId, String trackingNumber,
                    String carrier, String status, Instant createdAt,
                    Instant deliveredAt, boolean isNew) {
        this.shipmentId = shipmentId;
        this.orderId = orderId;
        this.trackingNumber = trackingNumber;
        this.carrier = carrier;
        this.status = status;
        this.createdAt = createdAt;
        this.deliveredAt = deliveredAt;
        this.isNew = isNew;
    }

    public static Shipment create(String orderId, String trackingNumber, String carrier) {
        String id = "SHIP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return new Shipment(id, orderId, trackingNumber, carrier, "CREATED", Instant.now(), null, true);
    }

    public Shipment markDelivered() {
        return new Shipment(shipmentId, orderId, trackingNumber, carrier, "DELIVERED", createdAt, Instant.now(), false);
    }

    @Override public String getId() { return shipmentId; }
    @Override public boolean isNew() { return isNew; }

    public String shipmentId()      { return shipmentId; }
    public String orderId()         { return orderId; }
    public String trackingNumber()  { return trackingNumber; }
    public String carrier()         { return carrier; }
    public String status()          { return status; }
    public Instant createdAt()      { return createdAt; }
    public Instant deliveredAt()    { return deliveredAt; }
}
