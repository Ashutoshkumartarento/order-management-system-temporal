package com.example.shipping.api.controller;

import com.example.contracts.api.ShippingContracts;
import com.example.shipping.application.service.ShippingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Tag(name = "Shipping", description = "Shipment creation and delivery confirmation")
public class ShippingController {

    private final ShippingService shippingService;

    public ShippingController(ShippingService shippingService) {
        this.shippingService = shippingService;
    }

    @Operation(summary = "Create a shipment for an order")
    @PostMapping("/shipments")
    public ResponseEntity<ShippingContracts.CreateShipmentResponse> createShipment(
            @Valid @RequestBody ShippingContracts.CreateShipmentRequest request) {
        return ResponseEntity.ok(shippingService.createShipment(request));
    }

    @Operation(summary = "Get shipment status by ID",
               description = "Used by ShippingActivityImpl to poll delivery status.")
    @GetMapping("/shipments/{shipmentId}")
    public ResponseEntity<ShippingContracts.ShipmentStatusResponse> getShipment(
            @PathVariable String shipmentId) {
        return ResponseEntity.ok(shippingService.getShipment(shipmentId));
    }

    @Operation(summary = "Confirm delivery (simulates carrier webhook)")
    @PostMapping("/deliveries/{shipmentId}/confirm")
    public ResponseEntity<ShippingContracts.ConfirmDeliveryResponse> confirmDelivery(
            @PathVariable String shipmentId) {
        return ResponseEntity.ok(shippingService.confirmDelivery(shipmentId));
    }
}