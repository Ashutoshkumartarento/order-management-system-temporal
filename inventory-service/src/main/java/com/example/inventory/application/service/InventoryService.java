package com.example.inventory.application.service;

import com.example.contracts.api.InventoryContracts;
import com.example.contracts.kafka.InventoryEventMessage;
import com.example.inventory.domain.model.InsufficientStockException;
import com.example.inventory.domain.model.InventoryItem;
import com.example.inventory.domain.model.Reservation;
import com.example.inventory.infrastructure.persistence.InventoryItemRepository;
import com.example.inventory.infrastructure.persistence.ReservationRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class InventoryService {

    private final ReservationRepository    reservationRepository;
    private final InventoryItemRepository  inventoryItemRepository;
    private final ObjectMapper             objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    @Value("${simulation.inventory-failure-rate:0.0}")
    private double failureRate;

    public InventoryService(ReservationRepository reservationRepository,
                            InventoryItemRepository inventoryItemRepository,
                            ObjectMapper objectMapper,
                            ApplicationEventPublisher eventPublisher) {
        this.reservationRepository   = reservationRepository;
        this.inventoryItemRepository = inventoryItemRepository;
        this.objectMapper            = objectMapper;
        this.eventPublisher          = eventPublisher;
    }

    @Transactional
    public InventoryContracts.ReserveInventoryResponse reserve(
            InventoryContracts.ReserveInventoryRequest request) {

        // Idempotency: already reserved for this order
        return reservationRepository.findByOrderId(request.orderId())
                .map(existing -> new InventoryContracts.ReserveInventoryResponse(
                        existing.reservationId(), "RESERVED", "Already reserved (idempotent)"))
                .orElseGet(() -> {
                    simulateFailure(request.orderId());

                    List<InventoryContracts.ReserveInventoryRequest.LineItem> items =
                            request.items() != null ? request.items() : List.of();

                    // Check and decrement stock for each requested product
                    for (var lineItem : items) {
                        InventoryItem item = inventoryItemRepository
                                .findById(lineItem.productId().toString())
                                .orElseThrow(() -> new InsufficientStockException(
                                        "Unknown product: " + lineItem.productId()));
                        inventoryItemRepository.save(item.reserve(lineItem.quantity()));
                    }

                    String itemsJson = serializeItems(items);
                    Reservation reservation = Reservation.create(request.orderId(), itemsJson);
                    reservationRepository.save(reservation);

                    eventPublisher.publishEvent(new InventoryEventMessage.InventoryReservedMessage(
                            UUID.randomUUID(), request.orderId(),
                            reservation.reservationId(), Instant.now()));

                    return new InventoryContracts.ReserveInventoryResponse(
                            reservation.reservationId(), "RESERVED", "Inventory reserved successfully");
                });
    }

    @Transactional
    public InventoryContracts.ReleaseInventoryResponse release(String reservationId) {
        reservationRepository.findById(reservationId).ifPresent(reservation -> {
            // Restore stock for each item that was originally reserved
            List<LineItemRecord> items = deserializeItems(reservation.itemsJson());
            for (var lineItem : items) {
                inventoryItemRepository.findById(lineItem.productId())
                        .ifPresent(item -> inventoryItemRepository.save(item.release(lineItem.quantity())));
            }
            reservationRepository.save(reservation.release());

            eventPublisher.publishEvent(new InventoryEventMessage.InventoryReleasedMessage(
                    UUID.randomUUID(), reservation.orderId(),
                    reservationId, "Saga compensation", Instant.now()));
        });

        return new InventoryContracts.ReleaseInventoryResponse(reservationId, "RELEASED");
    }

    // ─── CRUD operations ───────────────────────────────────────────────

    public List<InventoryItem> getAllItems() {
        List<InventoryItem> result = new ArrayList<>();
        inventoryItemRepository.findAll().forEach(result::add);
        return result;
    }

    public InventoryItem getItem(String productId) {
        return inventoryItemRepository.findById(productId)
                .orElseThrow(() -> new NoSuchElementException("Product not found: " + productId));
    }

    @Transactional
    public InventoryItem createItem(String productId, String productName, int quantity) {
        if (inventoryItemRepository.existsById(productId)) {
            throw new IllegalStateException("Product already exists: " + productId);
        }
        return inventoryItemRepository.save(InventoryItem.create(productId, productName, quantity));
    }

    @Transactional
    public InventoryItem updateItem(String productId, String productName, Integer quantityAvailable) {
        InventoryItem existing = inventoryItemRepository.findById(productId)
                .orElseThrow(() -> new NoSuchElementException("Product not found: " + productId));
        String newName = productName != null ? productName : existing.productName();
        int newQty = quantityAvailable != null ? quantityAvailable : existing.quantityAvailable();
        return inventoryItemRepository.save(
                new InventoryItem(productId, newName, newQty, existing.quantityReserved(), Instant.now()));
    }

    @Transactional
    public void deleteItem(String productId) {
        if (!inventoryItemRepository.existsById(productId)) {
            throw new NoSuchElementException("Product not found: " + productId);
        }
        inventoryItemRepository.deleteById(productId);
    }

    // ───────────────────────────────────────────────────────────────────

    private void simulateFailure(String orderId) {
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new RuntimeException(
                    "Inventory service unavailable (simulated, rate=" + failureRate + ")");
        }
    }

    private String serializeItems(
            List<InventoryContracts.ReserveInventoryRequest.LineItem> items) {
        try {
            List<LineItemRecord> records = items.stream()
                    .map(li -> new LineItemRecord(li.productId().toString(), li.quantity()))
                    .toList();
            return objectMapper.writeValueAsString(records);
        } catch (JsonProcessingException e) {
            return "[]";
        }
    }

    private List<LineItemRecord> deserializeItems(String json) {
        if (json == null || json.isBlank() || json.equals("[]")) return List.of();
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            return List.of();
        }
    }

    record LineItemRecord(String productId, int quantity) {}
}