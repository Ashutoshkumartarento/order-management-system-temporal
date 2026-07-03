package com.example.ordermanagement.api.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * REST Request DTO for POST /orders
 *
 * DTOs are the API contract — they're decoupled from domain objects.
 * If the domain model changes (e.g., shipping address becomes a structured object),
 * the API contract can stay stable or evolve independently.
 */
public record CreateOrderRequest(
        @NotNull(message = "customerId is required")
        UUID customerId,

        @NotBlank(message = "shippingAddress is required")
        String shippingAddress
) {}
