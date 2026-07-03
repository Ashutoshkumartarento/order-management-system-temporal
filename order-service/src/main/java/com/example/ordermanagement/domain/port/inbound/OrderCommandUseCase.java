package com.example.ordermanagement.domain.port.inbound;

import com.example.ordermanagement.domain.command.*;
import com.example.ordermanagement.domain.valueobject.OrderId;

/**
 * Inbound Port: OrderCommandUseCase
 *
 * Defines all write operations available on orders.
 * Controllers depend on this interface, not on OrderCommandService directly,
 * keeping the hexagonal boundary intact.
 */
public interface OrderCommandUseCase {

    OrderId createOrder(CreateOrderCommand command);

    void addItem(AddItemCommand command);

    void removeItem(RemoveItemCommand command);

    void confirmOrder(ConfirmOrderCommand command);

    void cancelOrder(CancelOrderCommand command);

    void recordPaymentCompleted(OrderId orderId, String transactionId);
}
