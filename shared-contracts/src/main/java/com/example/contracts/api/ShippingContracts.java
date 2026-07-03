package com.example.contracts.api;

import jakarta.validation.constraints.NotBlank;

/**
 * HTTP API contracts for the Shipping Service.
 */
public final class ShippingContracts {

    private ShippingContracts() {}

    public record CreateShipmentRequest(
            @NotBlank String orderId,
            @NotBlank String shippingAddress
    ) {}

    public record CreateShipmentResponse(
            String shipmentId,
            String trackingNumber,
            String carrier,
            String status   // "CREATED"
    ) {}

    public record ShipmentStatusResponse(
            String shipmentId,
            String status,      // "CREATED" | "IN_TRANSIT" | "DELIVERED"
            String trackingNumber,
            String carrier
    ) {}

    public record ConfirmDeliveryResponse(
            String shipmentId,
            String status       // "DELIVERED"
    ) {}
}
