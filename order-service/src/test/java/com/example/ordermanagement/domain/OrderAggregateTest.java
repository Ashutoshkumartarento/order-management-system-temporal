package com.example.ordermanagement.domain;

import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.aggregate.OrderSnapshot;
import com.example.ordermanagement.domain.event.*;
import com.example.ordermanagement.domain.exception.DomainException;
import com.example.ordermanagement.domain.exception.InvalidStateTransitionException;
import com.example.ordermanagement.domain.model.OrderStatus;
import com.example.ordermanagement.domain.model.PaymentStatus;
import com.example.ordermanagement.domain.valueobject.*;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit Tests: OrderAggregate
 *
 * Tests the core domain aggregate in complete isolation.
 * No Spring context, no DB, no Temporal — pure domain logic.
 *
 * TESTING STRATEGY:
 * 1. Command handling tests: verify correct events are raised
 * 2. State reconstruction tests: verify replay works correctly
 * 3. Business rule tests: verify invalid operations are rejected
 * 4. Snapshot tests: verify snapshot round-trip works
 * 5. Optimistic locking tests: verify version conflict detection
 *
 * These tests run extremely fast (<1 second) because there's no infrastructure.
 */
@DisplayName("Order Aggregate Tests")
class OrderAggregateTest {

    static final OrderId ORDER_ID = OrderId.generate();
    static final CustomerId CUSTOMER_ID = CustomerId.generate();
    static final String ADDRESS = "123 Test Street, Test City, TC 12345";

    @Nested
    @DisplayName("Order Creation")
    class Creation {

        @Test
        @DisplayName("Should create order with DRAFT status")
        void shouldCreateOrderInDraftStatus() {
            Order order = Order.create(ORDER_ID, CUSTOMER_ID, ADDRESS);

            assertThat(order.getStatus()).isEqualTo(OrderStatus.DRAFT);
            assertThat(order.getId()).isEqualTo(ORDER_ID);
            assertThat(order.getCustomerId()).isEqualTo(CUSTOMER_ID);
            assertThat(order.getItems()).isEmpty();
            assertThat(order.getVersion()).isEqualTo(1L);
        }

        @Test
        @DisplayName("Should raise OrderCreatedEvent on creation")
        void shouldRaiseOrderCreatedEvent() {
            Order order = Order.create(ORDER_ID, CUSTOMER_ID, ADDRESS);
            List<DomainEvent> events = order.drainPendingEvents();

            assertThat(events).hasSize(1);
            assertThat(events.getFirst()).isInstanceOf(OrderCreatedEvent.class);

            OrderCreatedEvent event = (OrderCreatedEvent) events.getFirst();
            assertThat(event.aggregateId()).isEqualTo(ORDER_ID.toString());
            assertThat(event.customerId()).isEqualTo(CUSTOMER_ID);
            assertThat(event.version()).isEqualTo(1L);
        }

        @Test
        @DisplayName("Should reject empty shipping address")
        void shouldRejectEmptyAddress() {
            assertThatThrownBy(() -> Order.create(ORDER_ID, CUSTOMER_ID, ""))
                    .isInstanceOf(DomainException.class)
                    .hasMessageContaining("Shipping address is required");
        }
    }

    @Nested
    @DisplayName("Item Management")
    class ItemManagement {

        @Test
        @DisplayName("Should add item to DRAFT order")
        void shouldAddItemToDraftOrder() {
            Order order = createDraftOrder();
            order.drainPendingEvents(); // Clear creation event

            OrderItem item = createItem("Widget", 2, 10.00);
            order.addItem(item);

            assertThat(order.getItems()).hasSize(1);
            assertThat(order.getTotalAmount()).isEqualTo(Money.of(20.00));

            List<DomainEvent> events = order.drainPendingEvents();
            assertThat(events.stream().filter(e -> e instanceof ItemAddedEvent)).hasSize(1);
        }

        @Test
        @DisplayName("Should merge duplicate products by increasing quantity")
        void shouldMergeDuplicateProducts() {
            Order order = createDraftOrder();
            UUID productId = UUID.randomUUID();

            order.addItem(new OrderItem(productId, "Widget", 2, Money.of(10.00)));
            order.addItem(new OrderItem(productId, "Widget", 3, Money.of(10.00)));
            order.drainPendingEvents();

            // After merge, should have 1 item with quantity 5
            assertThat(order.getItems()).hasSize(1);
            assertThat(order.getItems().getFirst().quantity()).isEqualTo(5);
            assertThat(order.getTotalAmount()).isEqualTo(Money.of(50.00));
        }

        @Test
        @DisplayName("Should reject adding items to CONFIRMED order")
        void shouldRejectAddingItemsToConfirmedOrder() {
            Order order = createConfirmedOrder();

            assertThatThrownBy(() -> order.addItem(createItem("Widget", 1, 10.00)))
                    .isInstanceOf(InvalidStateTransitionException.class);
        }

        @Test
        @DisplayName("Should remove item from DRAFT order")
        void shouldRemoveItemFromDraftOrder() {
            Order order = createDraftOrder();
            UUID productId = UUID.randomUUID();
            order.addItem(new OrderItem(productId, "Widget", 2, Money.of(10.00)));
            order.drainPendingEvents();

            order.removeItem(productId);

            assertThat(order.getItems()).isEmpty();
            assertThat(order.getTotalAmount()).isEqualTo(Money.ZERO);
        }

        @Test
        @DisplayName("Should reject removing non-existent item")
        void shouldRejectRemovingNonExistentItem() {
            Order order = createDraftOrder();

            assertThatThrownBy(() -> order.removeItem(UUID.randomUUID()))
                    .isInstanceOf(DomainException.class)
                    .hasMessageContaining("not found in order");
        }
    }

    @Nested
    @DisplayName("Order Confirmation")
    class Confirmation {

        @Test
        @DisplayName("Should confirm order and update status")
        void shouldConfirmOrder() {
            Order order = createDraftOrder();
            order.addItem(createItem("Widget", 2, 10.00));
            order.drainPendingEvents();

            order.confirm("workflow-123");

            assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
            assertThat(order.getWorkflowId()).isEqualTo("workflow-123");
        }

        @Test
        @DisplayName("Should reject confirming empty order")
        void shouldRejectConfirmingEmptyOrder() {
            Order order = createDraftOrder();

            assertThatThrownBy(() -> order.confirm("workflow-123"))
                    .isInstanceOf(DomainException.class)
                    .hasMessageContaining("empty order");
        }

        @Test
        @DisplayName("Should reject double confirmation")
        void shouldRejectDoubleConfirmation() {
            Order order = createConfirmedOrder();

            assertThatThrownBy(() -> order.confirm("workflow-456"))
                    .isInstanceOf(InvalidStateTransitionException.class);
        }
    }

    @Nested
    @DisplayName("Fulfillment Steps")
    class FulfillmentSteps {

        @Test
        @DisplayName("Should record inventory reservation")
        void shouldRecordInventoryReservation() {
            Order order = createConfirmedOrder();
            order.drainPendingEvents();

            order.reserveInventory("RES-001");

            assertThat(order.getStatus()).isEqualTo(OrderStatus.INVENTORY_RESERVED);
            assertThat(order.getReservationId()).isEqualTo("RES-001");

            List<DomainEvent> events = order.drainPendingEvents();
            assertThat(events.getFirst()).isInstanceOf(InventoryReservedEvent.class);
        }

        @Test
        @DisplayName("Should record payment completion")
        void shouldRecordPaymentCompletion() {
            Order order = createInventoryReservedOrder();
            order.drainPendingEvents();

            order.completePayment("TXN-001", Money.of(20.00));

            assertThat(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_COMPLETED);
            assertThat(order.getPaymentStatus()).isEqualTo(PaymentStatus.COMPLETED);
            assertThat(order.getTransactionId()).isEqualTo("TXN-001");
        }

        @Test
        @DisplayName("Should record shipment and delivery")
        void shouldRecordShipmentAndDelivery() {
            Order order = createPaymentCompletedOrder();
            order.drainPendingEvents();

            order.createShipment("SHIP-001", "1ZTRACKING123", "UPS");
            assertThat(order.getStatus()).isEqualTo(OrderStatus.SHIPPED);

            order.deliverOrder("SHIP-001");
            assertThat(order.getStatus()).isEqualTo(OrderStatus.DELIVERED);
        }
    }

    @Nested
    @DisplayName("Event Replay (Reconstitution)")
    class Reconstitution {

        /**
         * This test verifies the CORE principle of event sourcing:
         * State can be rebuilt by replaying events.
         *
         * WHY IS THIS IMPORTANT?
         * If your aggregate can be reconstituted from events, you can:
         * 1. Rebuild state after system failures
         * 2. Travel back in time to any point in the past
         * 3. Build new read models from the same events
         * 4. Audit every state change that ever happened
         */
        @Test
        @DisplayName("Should reconstitute order from events")
        void shouldReconstituteFromEvents() {
            // Create an order through its lifecycle
            Order original = Order.create(ORDER_ID, CUSTOMER_ID, ADDRESS);
            UUID productId = UUID.randomUUID();
            original.addItem(new OrderItem(productId, "Widget", 2, Money.of(15.00)));
            original.confirm("workflow-replay-test");
            original.reserveInventory("RES-REPLAY");
            original.completePayment("TXN-REPLAY", original.getTotalAmount());
            original.createShipment("SHIP-REPLAY", "TRACK123", "UPS");

            // Collect all events
            List<DomainEvent> allEvents = original.drainPendingEvents();

            // Reconstitute a new order from those events
            Order reconstituted = Order.reconstitute(allEvents);

            // Verify all state matches
            assertThat(reconstituted.getId()).isEqualTo(original.getId());
            assertThat(reconstituted.getStatus()).isEqualTo(OrderStatus.SHIPPED);
            assertThat(reconstituted.getCustomerId()).isEqualTo(CUSTOMER_ID);
            assertThat(reconstituted.getTotalAmount()).isEqualTo(Money.of(30.00));
            assertThat(reconstituted.getReservationId()).isEqualTo("RES-REPLAY");
            assertThat(reconstituted.getTransactionId()).isEqualTo("TXN-REPLAY");
            assertThat(reconstituted.getTrackingNumber()).isEqualTo("TRACK123");
            assertThat(reconstituted.getVersion()).isEqualTo(original.getVersion());
        }

        @Test
        @DisplayName("Should fail to reconstitute from empty event list")
        void shouldFailWithEmptyEvents() {
            assertThatThrownBy(() -> Order.reconstitute(List.of()))
                    .isInstanceOf(DomainException.class);
        }
    }

    @Nested
    @DisplayName("Snapshot Support")
    class SnapshotSupport {

        /**
         * Verifies the snapshot → event replay optimization works correctly.
         * SNAPSHOT_THRESHOLD events are typically 50 in production.
         * Here we test the round-trip: create snapshot, restore, replay remaining events.
         */
        @Test
        @DisplayName("Should create and restore from snapshot")
        void shouldCreateAndRestoreFromSnapshot() {
            Order original = createShippedOrder();
            original.drainPendingEvents();

            // Take a snapshot
            OrderSnapshot snapshot = original.toSnapshot();
            assertThat(snapshot.aggregateId()).isEqualTo(ORDER_ID.toString());
            assertThat(snapshot.version()).isEqualTo(original.getVersion());
            assertThat(snapshot.status()).isEqualTo(OrderStatus.SHIPPED);

            // Restore from snapshot
            Order restored = Order.reconstituteFromSnapshot(snapshot, List.of());

            assertThat(restored.getId()).isEqualTo(original.getId());
            assertThat(restored.getStatus()).isEqualTo(original.getStatus());
            assertThat(restored.getVersion()).isEqualTo(original.getVersion());
            assertThat(restored.getTotalAmount()).isEqualTo(original.getTotalAmount());
        }

        @Test
        @DisplayName("Should restore from snapshot and replay subsequent events")
        void shouldRestoreAndReplayEvents() {
            Order order = createShippedOrder();
            List<DomainEvent> allEvents = order.drainPendingEvents();

            // Simulate: take snapshot at version 5, then deliver (version 6)
            OrderSnapshot snapshot = order.toSnapshot();
            order.deliverOrder(order.getShipmentId());
            List<DomainEvent> newEvents = order.drainPendingEvents();

            // Reconstitute from snapshot + new events
            Order restored = Order.reconstituteFromSnapshot(snapshot, newEvents);

            assertThat(restored.getStatus()).isEqualTo(OrderStatus.DELIVERED);
            assertThat(restored.getVersion()).isEqualTo(order.getVersion());
        }
    }

    @Nested
    @DisplayName("Saga Compensation")
    class SagaCompensation {

        @Test
        @DisplayName("Should cancel order from any non-terminal state")
        void shouldCancelFromAnyState() {
            // Cancel from DRAFT
            Order draft = createDraftOrder();
            draft.cancel("Changed mind", "CUSTOMER");
            assertThat(draft.getStatus()).isEqualTo(OrderStatus.CANCELLED);

            // Cancel from CONFIRMED
            Order confirmed = createConfirmedOrder();
            confirmed.cancel("Inventory failed", "SYSTEM_COMPENSATION");
            assertThat(confirmed.getStatus()).isEqualTo(OrderStatus.CANCELLED);

            // Cancel from INVENTORY_RESERVED
            Order reserved = createInventoryReservedOrder();
            reserved.cancel("Payment failed", "SYSTEM_COMPENSATION");
            assertThat(reserved.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        }

        @Test
        @DisplayName("Should reject cancellation from terminal states")
        void shouldRejectCancellingTerminalOrder() {
            Order delivered = createDeliveredOrder();

            assertThatThrownBy(() -> delivered.cancel("Too late", "CUSTOMER"))
                    .isInstanceOf(InvalidStateTransitionException.class)
                    .hasMessageContaining("terminal state");
        }

        @Test
        @DisplayName("Should release inventory as compensation")
        void shouldReleaseInventoryAsCompensation() {
            Order order = createInventoryReservedOrder();
            order.drainPendingEvents();

            order.releaseInventory("Payment failed");

            List<DomainEvent> events = order.drainPendingEvents();
            assertThat(events.getFirst()).isInstanceOf(InventoryReleasedEvent.class);
        }

        @Test
        @DisplayName("Should refund payment as compensation")
        void shouldRefundPaymentAsCompensation() {
            Order order = createPaymentCompletedOrder();
            order.drainPendingEvents();

            order.completeRefund("REFUND-001", Money.of(30.00));

            List<DomainEvent> events = order.drainPendingEvents();
            assertThat(events.getFirst()).isInstanceOf(RefundCompletedEvent.class);
            assertThat(order.getPaymentStatus()).isEqualTo(PaymentStatus.REFUNDED);
        }
    }

    // ─────────────────────────────────────────────────────
    // Test Fixture Helpers
    // ─────────────────────────────────────────────────────

    private Order createDraftOrder() {
        return Order.create(ORDER_ID, CUSTOMER_ID, ADDRESS);
    }

    private Order createConfirmedOrder() {
        Order order = createDraftOrder();
        order.addItem(createItem("Widget", 2, 15.00));
        order.confirm("workflow-test");
        return order;
    }

    private Order createInventoryReservedOrder() {
        Order order = createConfirmedOrder();
        order.reserveInventory("RES-TEST");
        return order;
    }

    private Order createPaymentCompletedOrder() {
        Order order = createInventoryReservedOrder();
        order.completePayment("TXN-TEST", Money.of(30.00));
        return order;
    }

    private Order createShippedOrder() {
        Order order = createPaymentCompletedOrder();
        order.createShipment("SHIP-TEST", "TRACK-TEST", "UPS");
        return order;
    }

    private Order createDeliveredOrder() {
        Order order = createShippedOrder();
        order.deliverOrder("SHIP-TEST");
        return order;
    }

    private OrderItem createItem(String name, int qty, double price) {
        return new OrderItem(UUID.randomUUID(), name, qty, Money.of(price));
    }
}
