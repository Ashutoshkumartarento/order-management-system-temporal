package com.example.ordermanagement.api.dto.response;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderSummaryResponse(
        String orderId,
        String customerId,
        String status,
        String paymentStatus,
        String shipmentStatus,
        BigDecimal totalAmount,
        int itemCount,
        String shippingAddress,
        String workflowId,
        String trackingNumber,
        Instant createdAt,
        Instant confirmedAt,
        Instant paidAt,
        Instant deliveredAt,
        Instant cancelledAt,
        String cancelReason,
        Instant updatedAt
) {}