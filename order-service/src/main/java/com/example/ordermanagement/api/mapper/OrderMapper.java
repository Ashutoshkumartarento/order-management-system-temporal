package com.example.ordermanagement.api.mapper;

import com.example.ordermanagement.api.dto.response.EventHistoryResponse;
import com.example.ordermanagement.api.dto.response.OrderResponse;
import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.event.DomainEvent;
import com.example.ordermanagement.domain.valueobject.OrderItem;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;

import java.util.List;

/**
 * MapStruct Mapper: OrderMapper
 *
 * Handles conversion between domain objects and API DTOs.
 * MapStruct generates implementation code at compile time.
 * No runtime reflection — fast and type-safe.
 *
 * The @Mapper(componentModel = "spring") annotation is set via
 * maven compiler args (-Amapstruct.defaultComponentModel=spring),
 * making this a Spring bean automatically.
 */
@Mapper
public interface OrderMapper {

    @Mapping(target = "orderId", expression = "java(order.getId().value())")
    @Mapping(target = "customerId", expression = "java(order.getCustomerId().value())")
    @Mapping(target = "status", expression = "java(order.getStatus().name())")
    @Mapping(target = "totalAmount", expression = "java(order.getTotalAmount().amount())")
    @Mapping(target = "currency", expression = "java(order.getTotalAmount().getCurrencyCode())")
    @Mapping(target = "paymentStatus", expression = "java(order.getPaymentStatus().name())")
    @Mapping(target = "shipmentStatus", expression = "java(order.getShipmentStatus().name())")
    @Mapping(target = "version", source = "version")
    @Mapping(target = "items", source = "items")
    OrderResponse toResponse(Order order);

    @Mapping(target = "unitPrice", expression = "java(item.unitPrice().amount())")
    @Mapping(target = "totalPrice", expression = "java(item.totalPrice().amount())")
    OrderResponse.OrderItemResponse toItemResponse(OrderItem item);

    default EventHistoryResponse toEventHistoryResponse(DomainEvent event) {
        return new EventHistoryResponse(
                event.eventId(),
                event.aggregateId(),
                event.eventType(),
                event.version(),
                event.occurredAt(),
                event  // Include full event as payload
        );
    }

    List<EventHistoryResponse> toEventHistoryResponses(List<DomainEvent> events);
}
