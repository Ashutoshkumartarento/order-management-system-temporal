package com.example.ordermanagement.domain.command;

import com.example.ordermanagement.domain.valueobject.CustomerId;
import com.example.ordermanagement.domain.valueobject.OrderId;

/**
 * Command: CreateOrderCommand
 *
 * Commands are INTENT — they represent what a user/system wants to do.
 * Commands can be rejected (validated, business rules checked).
 * Events cannot be rejected — they are facts that already happened.
 *
 * CQS/CQRS Principle:
 *   Commands change state, return nothing (or minimal acknowledgment).
 *   Queries read state, change nothing.
 */
public record CreateOrderCommand(
        OrderId orderId,
        CustomerId customerId,
        String shippingAddress
) {
    public CreateOrderCommand {
        if (orderId == null) orderId = OrderId.generate();
    }
}
