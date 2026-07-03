package com.example.contracts.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.UUID;

/**
 * HTTP API contracts for the Inventory Service.
 *
 * Used by:
 *   - InventoryActivityImpl (order-service) as request/response types
 *   - InventoryController (inventory-service) as endpoint types
 *
 * Keeping them in shared-contracts means both sides always agree
 * on the request/response schema without coupling their domains.
 */
public final class InventoryContracts {

    private InventoryContracts() {}

    public record ReserveInventoryRequest(
            @NotBlank String orderId,
            @NotNull List<LineItem> items
    ) {
        public record LineItem(UUID productId, int quantity) {}
    }

    public record ReserveInventoryResponse(
            String reservationId,
            String status,      // "RESERVED"
            String message
    ) {}

    public record ReleaseInventoryResponse(
            String reservationId,
            String status       // "RELEASED"
    ) {}
}
