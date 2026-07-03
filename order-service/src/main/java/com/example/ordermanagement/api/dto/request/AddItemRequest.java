package com.example.ordermanagement.api.dto.request;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

public record AddItemRequest(
        @NotNull UUID productId,
        @NotBlank String productName,
        @Min(1) int quantity,
        @NotNull @DecimalMin("0.01") BigDecimal unitPrice
) {}
