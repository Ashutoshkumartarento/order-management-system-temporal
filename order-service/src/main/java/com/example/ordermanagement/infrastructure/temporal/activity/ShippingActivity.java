package com.example.ordermanagement.infrastructure.temporal.activity;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: ShippingActivity
 *
 * Handles shipment creation and delivery tracking.
 * In a real system, this would integrate with carrier APIs (FedEx, UPS, etc.).
 */
@ActivityInterface
public interface ShippingActivity {

    /**
     * Creates a shipment with the carrier.
     * Returns tracking information.
     */
    @ActivityMethod
    ShipmentResult createShipment(String orderId);

    /**
     * Confirms delivery (simulates carrier webhook callback).
     * In reality, this would wait for an async signal from the carrier.
     */
    @ActivityMethod
    void confirmDelivery(String orderId, String shipmentId);

    @ActivityMethod
    void recordShipmentCreated(String orderId, String shipmentId, String trackingNumber, String carrier);

    @ActivityMethod
    void recordShipmentDelivered(String orderId, String shipmentId);

    record ShipmentResult(String shipmentId, String trackingNumber, String carrier) {}
}
