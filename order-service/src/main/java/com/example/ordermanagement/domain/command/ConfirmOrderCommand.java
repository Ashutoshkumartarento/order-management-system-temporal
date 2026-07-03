package com.example.ordermanagement.domain.command;

import com.example.ordermanagement.domain.valueobject.OrderId;

public record ConfirmOrderCommand(OrderId orderId) {}
