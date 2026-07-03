package com.example.inventory.api.controller;

import com.example.contracts.api.InventoryContracts;
import com.example.inventory.application.service.InventoryService;
import com.example.inventory.domain.model.InsufficientStockException;
import com.example.inventory.domain.model.InventoryItem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

@RestController
@Tag(name = "Inventory", description = "Stock reservation and release for order fulfillment")
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @Operation(summary = "Reserve inventory for an order",
               description = "Called by order-service InventoryActivity. " +
                             "Idempotent: same orderId always returns same reservationId.")
    @PostMapping("/reserve")
    public ResponseEntity<InventoryContracts.ReserveInventoryResponse> reserve(
            @Valid @RequestBody InventoryContracts.ReserveInventoryRequest request) {
        return ResponseEntity.ok(inventoryService.reserve(request));
    }

    @Operation(summary = "Release a reservation (saga compensation)")
    @DeleteMapping("/reserve/{reservationId}")
    public ResponseEntity<InventoryContracts.ReleaseInventoryResponse> release(
            @PathVariable String reservationId) {
        return ResponseEntity.ok(inventoryService.release(reservationId));
    }

    // ─── CRUD: Inventory Items ────────────────────────────────────────

    @Operation(summary = "List all inventory items")
    @GetMapping("/inventory")
    public ResponseEntity<List<InventoryItemResponse>> listItems() {
        return ResponseEntity.ok(
                inventoryService.getAllItems().stream().map(InventoryItemResponse::from).toList());
    }

    @Operation(summary = "Get a single inventory item by productId")
    @GetMapping("/inventory/{productId}")
    public ResponseEntity<InventoryItemResponse> getItem(@PathVariable String productId) {
        return ResponseEntity.ok(InventoryItemResponse.from(inventoryService.getItem(productId)));
    }

    @Operation(summary = "Create a new inventory item")
    @PostMapping("/inventory")
    public ResponseEntity<InventoryItemResponse> createItem(
            @Valid @RequestBody CreateItemRequest request) {
        InventoryItem created = inventoryService.createItem(
                request.productId(), request.productName(), request.quantityAvailable());
        return ResponseEntity.status(HttpStatus.CREATED).body(InventoryItemResponse.from(created));
    }

    @Operation(summary = "Update an inventory item's name or available quantity")
    @PutMapping("/inventory/{productId}")
    public ResponseEntity<InventoryItemResponse> updateItem(
            @PathVariable String productId,
            @RequestBody UpdateItemRequest request) {
        InventoryItem updated = inventoryService.updateItem(
                productId, request.productName(), request.quantityAvailable());
        return ResponseEntity.ok(InventoryItemResponse.from(updated));
    }

    @Operation(summary = "Delete an inventory item")
    @DeleteMapping("/inventory/{productId}")
    public ResponseEntity<Void> deleteItem(@PathVariable String productId) {
        inventoryService.deleteItem(productId);
        return ResponseEntity.noContent().build();
    }

    // ─── Exception handlers ───────────────────────────────────────────

    @ExceptionHandler(InsufficientStockException.class)
    public ResponseEntity<String> handleInsufficientStock(InsufficientStockException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(ex.getMessage());
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<String> handleNotFound(NoSuchElementException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<String> handleConflict(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getMessage());
    }

    // ─── DTOs ─────────────────────────────────────────────────────────

    record CreateItemRequest(
            @NotBlank String productId,
            @NotBlank String productName,
            @Min(0)   int quantityAvailable) {}

    record UpdateItemRequest(
            String  productName,        // null = keep existing
            Integer quantityAvailable)  {} // null = keep existing

    record InventoryItemResponse(
            String productId,
            String productName,
            int    quantityAvailable,
            int    quantityReserved,
            Instant updatedAt) {
        static InventoryItemResponse from(InventoryItem item) {
            return new InventoryItemResponse(
                    item.productId(), item.productName(),
                    item.quantityAvailable(), item.quantityReserved(),
                    item.updatedAt());
        }
    }
}
