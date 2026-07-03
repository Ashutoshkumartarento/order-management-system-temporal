package com.example.ordermanagement.domain.command;

import com.example.ordermanagement.domain.valueobject.OrderId;

public record CancelOrderCommand(
        OrderId orderId,
        String reason
) {}
