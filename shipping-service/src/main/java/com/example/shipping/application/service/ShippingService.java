package com.example.shipping.application.service;

import com.example.contracts.api.ShippingContracts;
import com.example.contracts.kafka.ShippingEventMessage;
import com.example.shipping.domain.model.Delivery;
import com.example.shipping.domain.model.Shipment;
import com.example.shipping.infrastructure.persistence.DeliveryRepository;
import com.example.shipping.infrastructure.persistence.ShipmentRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class ShippingService {

    private final ShipmentRepository   shipmentRepository;
    private final DeliveryRepository   deliveryRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Value("${simulation.shipping-failure-rate:0.0}")
    private double failureRate;

    public ShippingService(ShipmentRepository shipmentRepository,
                           DeliveryRepository deliveryRepository,
                           ApplicationEventPublisher eventPublisher) {
        this.shipmentRepository = shipmentRepository;
        this.deliveryRepository = deliveryRepository;
        this.eventPublisher     = eventPublisher;
    }

    @Transactional
    public ShippingContracts.CreateShipmentResponse createShipment(
            ShippingContracts.CreateShipmentRequest request) {

        // Idempotency: return existing shipment if already created for this order
        var existing = shipmentRepository.findByOrderId(request.orderId());
        if (existing.isPresent()) {
            Shipment s = existing.get();
            return new ShippingContracts.CreateShipmentResponse(
                    s.shipmentId(), s.trackingNumber(), s.carrier(), s.status());
        }

        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new RuntimeException("Carrier service unavailable (simulated)");
        }

        String tracking = "1Z" + UUID.randomUUID().toString().replace("-", "")
                .substring(0, 16).toUpperCase();
        String carrier = ThreadLocalRandom.current().nextBoolean() ? "UPS" : "FedEx";

        Shipment shipment = Shipment.create(request.orderId(), tracking, carrier);
        shipmentRepository.save(shipment);

        eventPublisher.publishEvent(new ShippingEventMessage.ShipmentCreatedMessage(
                UUID.randomUUID(), request.orderId(),
                shipment.shipmentId(), tracking, carrier, Instant.now()));

        return new ShippingContracts.CreateShipmentResponse(
                shipment.shipmentId(), tracking, carrier, "CREATED");
    }

    public ShippingContracts.ShipmentStatusResponse getShipment(String shipmentId) {
        Shipment s = shipmentRepository.findById(shipmentId)
                .orElseThrow(() -> new RuntimeException("Shipment not found: " + shipmentId));
        return new ShippingContracts.ShipmentStatusResponse(
                s.shipmentId(), s.status(), s.trackingNumber(), s.carrier());
    }

    @Transactional
    public ShippingContracts.ConfirmDeliveryResponse confirmDelivery(String shipmentId) {
        Shipment shipment = shipmentRepository.findById(shipmentId)
                .orElseThrow(() -> new RuntimeException("Shipment not found: " + shipmentId));

        // Idempotency: already confirmed
        var existing = deliveryRepository.findByShipmentId(shipmentId);
        if (existing.isPresent()) {
            return new ShippingContracts.ConfirmDeliveryResponse(shipmentId, "DELIVERED");
        }

        // Mark shipment as delivered and persist both records atomically
        Shipment delivered = shipment.markDelivered();
        shipmentRepository.save(delivered);
        deliveryRepository.save(Delivery.create(shipmentId, shipment.orderId()));

        eventPublisher.publishEvent(new ShippingEventMessage.ShipmentDeliveredMessage(
                UUID.randomUUID(), shipment.orderId(),
                shipmentId, Instant.now(), Instant.now()));

        return new ShippingContracts.ConfirmDeliveryResponse(shipmentId, "DELIVERED");
    }
}