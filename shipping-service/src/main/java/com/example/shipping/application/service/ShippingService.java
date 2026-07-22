package com.example.shipping.application.service;

import com.example.contracts.api.ShippingContracts;
import com.example.contracts.kafka.ShippingEventMessage;
import com.example.shipping.domain.model.Delivery;
import com.example.shipping.domain.model.Shipment;
import com.example.shipping.infrastructure.outbox.OutboxEntry;
import com.example.shipping.infrastructure.outbox.OutboxFlushSignal;
import com.example.shipping.infrastructure.outbox.OutboxRepository;
import com.example.shipping.infrastructure.persistence.DeliveryRepository;
import com.example.shipping.infrastructure.persistence.ShipmentRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class ShippingService {

    private final ShipmentRepository  shipmentRepository;
    private final DeliveryRepository  deliveryRepository;
    private final OutboxRepository    outboxRepository;
    private final ObjectMapper        objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    @Value("${simulation.shipping-failure-rate:0.0}")
    private double failureRate;

    public ShippingService(ShipmentRepository shipmentRepository,
                           DeliveryRepository deliveryRepository,
                           OutboxRepository outboxRepository,
                           ObjectMapper objectMapper,
                           ApplicationEventPublisher eventPublisher) {
        this.shipmentRepository = shipmentRepository;
        this.deliveryRepository = deliveryRepository;
        this.outboxRepository   = outboxRepository;
        this.objectMapper       = objectMapper;
        this.eventPublisher     = eventPublisher;
    }

    @Transactional
    public ShippingContracts.CreateShipmentResponse createShipment(
            ShippingContracts.CreateShipmentRequest request) {

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

        writeOutbox(new ShippingEventMessage.ShipmentCreatedMessage(
                UUID.randomUUID(), request.orderId(),
                shipment.shipmentId(), tracking, carrier, Instant.now()),
                "ShipmentCreated", "shipping.events");

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

        var existing = deliveryRepository.findByShipmentId(shipmentId);
        if (existing.isPresent()) {
            return new ShippingContracts.ConfirmDeliveryResponse(shipmentId, "DELIVERED");
        }

        Shipment delivered = shipment.markDelivered();
        shipmentRepository.save(delivered);
        deliveryRepository.save(Delivery.create(shipmentId, shipment.orderId()));

        writeOutbox(new ShippingEventMessage.ShipmentDeliveredMessage(
                UUID.randomUUID(), shipment.orderId(),
                shipmentId, Instant.now(), Instant.now()),
                "ShipmentDelivered", "shipping.events");

        return new ShippingContracts.ConfirmDeliveryResponse(shipmentId, "DELIVERED");
    }

    // ───────────────────────────────────────────────────────────────────

    private void writeOutbox(ShippingEventMessage message, String eventType, String topic) {
        try {
            String payload = objectMapper.writeValueAsString(message);
            outboxRepository.save(OutboxEntry.create(message.eventId(), message.orderId(),
                    eventType, topic, payload));
            eventPublisher.publishEvent(new OutboxFlushSignal());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize outbox event", e);
        }
    }
}
