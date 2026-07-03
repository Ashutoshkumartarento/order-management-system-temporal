package com.example.ordermanagement.domain.port.inbound;

import com.example.ordermanagement.api.dto.response.OrderSummaryResponse;
import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.event.DomainEvent;
import com.example.ordermanagement.domain.port.outbound.WorkflowPort;
import com.example.ordermanagement.domain.valueobject.OrderId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;

/**
 * Inbound Port: OrderQueryUseCase
 *
 * Defines all read operations available on orders.
 * Query side of CQRS — read-only, no state changes.
 * Controllers depend on this interface, not on OrderQueryService directly.
 */
public interface OrderQueryUseCase {

    Order getOrder(OrderId orderId);

    List<DomainEvent> getOrderHistory(OrderId orderId);

    List<TimelineEntry> getOrderTimeline(OrderId orderId);

    WorkflowPort.WorkflowStatusResult getWorkflowStatus(String workflowId);

    Page<OrderSummaryResponse> listOrders(List<String> statuses, String customerId, Pageable pageable);

    record TimelineEntry(long version, String eventType, String occurredAt, String description) {}
}
