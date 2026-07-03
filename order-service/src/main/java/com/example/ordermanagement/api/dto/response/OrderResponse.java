package com.example.ordermanagement.api.dto.response;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/**
 * REST Response DTO for Order state.
 * This is a "view model" — optimized for API consumers, not domain logic.
 */
public record OrderResponse(
        UUID orderId,
        UUID customerId,
        String status,
        List<OrderItemResponse> items,
        BigDecimal totalAmount,
        String currency,
        String paymentStatus,
        String shipmentStatus,
        String shippingAddress,
        String workflowId,
        String trackingNumber,
        long version
) {
    public record OrderItemResponse(
            UUID productId,
            String productName,
            int quantity,
            BigDecimal unitPrice,
            BigDecimal totalPrice
    ) {}
}
