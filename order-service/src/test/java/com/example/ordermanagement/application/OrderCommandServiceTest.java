package com.example.ordermanagement.application;

import com.example.ordermanagement.application.service.OrderCommandService;
import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.command.*;
import com.example.ordermanagement.domain.exception.OrderNotFoundException;
import com.example.ordermanagement.domain.model.OrderStatus;
import com.example.ordermanagement.domain.port.inbound.OrderCommandUseCase;
import com.example.ordermanagement.domain.port.outbound.OrderRepository;
import com.example.ordermanagement.domain.port.outbound.WorkflowPort;
import com.example.ordermanagement.domain.valueobject.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit Tests: OrderCommandService
 *
 * Tests the application service in isolation by mocking:
 * - OrderRepository (no DB)
 * - WorkflowPort (no Temporal)
 * - MeterRegistry (no metrics infrastructure)
 *
 * This tests the ORCHESTRATION logic: does the service correctly
 * call the aggregate, save events, and trigger workflows?
 */
@DisplayName("OrderCommandService Tests")
class OrderCommandServiceTest {

    OrderRepository repository;
    WorkflowPort workflowPort;
    MeterRegistry meterRegistry;
    OrderCommandUseCase service;
    OrderCommandService serviceImpl; // needed for internal methods not on the interface

    static final OrderId ORDER_ID = OrderId.generate();
    static final CustomerId CUSTOMER_ID = CustomerId.generate();

    @BeforeEach
    void setUp() {
        repository = mock(OrderRepository.class);
        workflowPort = mock(WorkflowPort.class);
        meterRegistry = new SimpleMeterRegistry();
        serviceImpl = new OrderCommandService(repository, workflowPort, meterRegistry);
        service = serviceImpl;
    }

    @Test
    @DisplayName("createOrder: should create order and save")
    void shouldCreateOrder() {
        CreateOrderCommand cmd = new CreateOrderCommand(ORDER_ID, CUSTOMER_ID, "Test Address");

        OrderId result = service.createOrder(cmd);

        assertThat(result).isEqualTo(ORDER_ID);
        verify(repository, times(1)).save(any(Order.class));
        verify(workflowPort, never()).startFulfillmentWorkflow(any(), any());
    }

    @Test
    @DisplayName("confirmOrder: should save confirmation and start workflow")
    void shouldConfirmOrderAndStartWorkflow() {
        Order order = Order.create(ORDER_ID, CUSTOMER_ID, "Test Address");
        order.addItem(new OrderItem(UUID.randomUUID(), "Widget", 2, Money.of(10.00)));
        order.drainPendingEvents(); // Clear so repo returns clean order

        when(repository.findById(ORDER_ID)).thenReturn(Optional.of(order));
        when(workflowPort.startFulfillmentWorkflow(eq(ORDER_ID), anyString()))
                .thenReturn("order-fulfillment-" + ORDER_ID);

        service.confirmOrder(new ConfirmOrderCommand(ORDER_ID));

        verify(repository).save(order);
        verify(workflowPort).startFulfillmentWorkflow(eq(ORDER_ID), anyString());
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    @DisplayName("cancelOrder: should signal Temporal and NOT save directly when workflow is active")
    void shouldCancelAndSignalWorkflow() {
        Order order = Order.create(ORDER_ID, CUSTOMER_ID, "Test Address");
        order.addItem(new OrderItem(UUID.randomUUID(), "Widget", 1, Money.of(5.00)));
        order.confirm("workflow-cancel-test");
        order.drainPendingEvents();

        when(repository.findById(ORDER_ID)).thenReturn(Optional.of(order));

        service.cancelOrder(new CancelOrderCommand(ORDER_ID, "Changed mind"));

        // Delegate entirely to Temporal — workflow owns compensation and the CANCELLED transition.
        // Writing CANCELLED here while mid-flight would break subsequent activity state transitions.
        verify(workflowPort).sendCancelSignal("workflow-cancel-test", "Changed mind");
        verify(repository, never()).save(any());
        // Order stays CONFIRMED in the domain — Temporal calls recordOrderCancelled later
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    @DisplayName("cancelOrder: should cancel without signal if no workflow")
    void shouldCancelWithoutSignalIfNoWorkflow() {
        Order order = Order.create(ORDER_ID, CUSTOMER_ID, "Test Address");
        order.drainPendingEvents();

        when(repository.findById(ORDER_ID)).thenReturn(Optional.of(order));

        service.cancelOrder(new CancelOrderCommand(ORDER_ID, "Draft cancelled"));

        verify(workflowPort, never()).sendCancelSignal(any(), any());
    }

    @Test
    @DisplayName("Should throw OrderNotFoundException for missing order")
    void shouldThrowForMissingOrder() {
        when(repository.findById(any())).thenReturn(Optional.empty());

        assertThatThrownBy(() ->
                service.confirmOrder(new ConfirmOrderCommand(ORDER_ID)))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    @DisplayName("recordInventoryReserved: already in INVENTORY_RESERVED state is handled")
    void idempotentInventoryReserved() {
        // The service should handle the state gracefully.
        // If the order is already INVENTORY_RESERVED, the aggregate throws
        // InvalidStateTransitionException which is the expected behavior —
        // it means the event was already applied. Callers (Temporal activities)
        // catch this and treat it as idempotent.
        Order order = Order.create(ORDER_ID, CUSTOMER_ID, "Test Address");
        order.addItem(new OrderItem(UUID.randomUUID(), "Widget", 1, Money.of(5.00)));
        order.confirm("wf-idem");
        order.reserveInventory("RES-ALREADY"); // already INVENTORY_RESERVED
        order.drainPendingEvents();

        when(repository.findById(ORDER_ID)).thenReturn(Optional.of(order));

        // Service propagates the domain exception — callers handle idempotency
        assertThatThrownBy(() ->
                serviceImpl.recordInventoryReserved(ORDER_ID, "RES-ALREADY"))
                .isInstanceOf(com.example.ordermanagement.domain.exception.InvalidStateTransitionException.class);
    }
}
