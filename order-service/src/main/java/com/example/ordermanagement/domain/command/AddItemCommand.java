package com.example.ordermanagement.domain.command;

import com.example.ordermanagement.domain.valueobject.Money;
import com.example.ordermanagement.domain.valueobject.OrderId;

import java.util.UUID;

public record AddItemCommand(
        OrderId orderId,
        UUID productId,
        String productName,
        int quantity,
        Money unitPrice
) {}
