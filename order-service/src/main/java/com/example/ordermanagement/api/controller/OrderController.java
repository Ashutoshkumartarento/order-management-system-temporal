package com.example.ordermanagement.api.controller;

import com.example.ordermanagement.api.dto.request.AddItemRequest;
import com.example.ordermanagement.api.dto.request.CancelOrderRequest;
import com.example.ordermanagement.api.dto.request.CreateOrderRequest;
import com.example.ordermanagement.api.dto.response.EventHistoryResponse;
import com.example.ordermanagement.api.dto.response.OrderResponse;
import com.example.ordermanagement.api.dto.response.OrderSummaryResponse;
import com.example.ordermanagement.api.mapper.OrderMapper;
import com.example.ordermanagement.domain.port.inbound.OrderCommandUseCase;
import com.example.ordermanagement.domain.port.inbound.OrderQueryUseCase;
import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.command.*;
import com.example.ordermanagement.domain.event.DomainEvent;
import com.example.ordermanagement.domain.port.outbound.WorkflowPort;
import com.example.ordermanagement.domain.valueobject.CustomerId;
import com.example.ordermanagement.domain.valueobject.Money;
import com.example.ordermanagement.domain.valueobject.OrderId;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST Controller: OrderController
 *
 * No business logic here — delegates everything to application services.
 * Business rules live in the Order aggregate.
 * Workflow orchestration lives in Temporal.
 */
@RestController
@RequestMapping("/orders")
@Tag(name = "Orders", description = """
        Order lifecycle management. State is **never stored directly** — it is
        always rebuilt by replaying immutable domain events from the append-only
        event store (PostgreSQL JSONB). Use `/history` and `/timeline` to see
        every state change that ever happened.
        """)
public class OrderController {

    private final OrderCommandUseCase commandService;
    private final OrderQueryUseCase queryService;
    private final OrderMapper orderMapper;
    private final WorkflowPort workflowPort;

    public OrderController(OrderCommandUseCase commandService, OrderQueryUseCase queryService,
                           OrderMapper orderMapper, WorkflowPort workflowPort) {
        this.commandService = commandService;
        this.queryService = queryService;
        this.orderMapper = orderMapper;
        this.workflowPort = workflowPort;
    }

    // ═══════════════════════════════════════════════════════
    // COMMAND ENDPOINTS
    // ═══════════════════════════════════════════════════════

    @Operation(
        summary = "Create a new order",
        description = "Creates an order in **DRAFT** status. Returns the new `orderId`. " +
                      "Stored event: `OrderCreatedEvent` (version 1).")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Order created — Location header points to the new resource"),
        @ApiResponse(responseCode = "400", description = "Validation error — customerId or shippingAddress missing")
    })
    @PostMapping
    public ResponseEntity<Map<String, String>> createOrder(
            @Valid @RequestBody CreateOrderRequest request) {

// Logging removed
        CreateOrderCommand command = new CreateOrderCommand(
                OrderId.generate(),
                CustomerId.of(request.customerId()),
                request.shippingAddress());
        OrderId orderId = commandService.createOrder(command);
        return ResponseEntity
                .created(URI.create("/orders/" + orderId))
                .body(Map.of("orderId", orderId.toString()));
    }

    @Operation(
        summary = "Add an item to a DRAFT order",
        description = "Adds a product line item. Duplicate productIds are merged (quantities summed). " +
                      "Stored event: `ItemAddedEvent`. Only valid while order is in **DRAFT** status.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Item added"),
        @ApiResponse(responseCode = "400", description = "Validation error or order not in DRAFT status"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @PostMapping("/{orderId}/items")
    public ResponseEntity<Void> addItem(
            @Parameter(description = "Order UUID", required = true) @PathVariable UUID orderId,
            @Valid @RequestBody AddItemRequest request) {

// Logging removed
        AddItemCommand command = new AddItemCommand(
                OrderId.of(orderId),
                request.productId(),
                request.productName(),
                request.quantity(),
                Money.of(request.unitPrice()));
        commandService.addItem(command);
        return ResponseEntity.ok().build();
    }

    @Operation(
        summary = "Remove an item from a DRAFT order",
        description = "Removes a product line item by productId. " +
                      "Stored event: `ItemRemovedEvent`. Only valid while order is in **DRAFT** status.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Item removed"),
        @ApiResponse(responseCode = "400", description = "Product not found in order, or order not in DRAFT"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @DeleteMapping("/{orderId}/items/{productId}")
    public ResponseEntity<Void> removeItem(
            @PathVariable UUID orderId,
            @PathVariable UUID productId) {

        commandService.removeItem(new RemoveItemCommand(OrderId.of(orderId), productId));
        return ResponseEntity.ok().build();
    }

    @Operation(
        summary = "Confirm the order and start Temporal workflow",
        description = """
                Transitions order from **DRAFT → CONFIRMED** and launches the
                `OrderFulfillmentWorkflow` in Temporal.
                
                The workflow orchestrates: Reserve Inventory → Process Payment →
                Create Shipment → Deliver, with automatic saga compensation on failures.
                
                Returns **202 Accepted** because fulfillment is asynchronous.
                Poll `GET /orders/{id}` or `GET /workflows/{workflowId}` to track progress.
                
                Stored event: `OrderConfirmedEvent` (includes workflowId).
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "Order confirmed — Temporal workflow started"),
        @ApiResponse(responseCode = "400", description = "Order is empty or not in DRAFT status"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @PostMapping("/{orderId}/confirm")
    public ResponseEntity<Map<String, String>> confirmOrder(
            @Parameter(description = "Order UUID", required = true) @PathVariable UUID orderId) {

// Logging removed
        commandService.confirmOrder(new ConfirmOrderCommand(OrderId.of(orderId)));
        return ResponseEntity.accepted()
                .body(Map.of(
                        "message", "Order confirmed. Fulfillment workflow started.",
                        "orderId", orderId.toString(),
                        "workflowId", "order-fulfillment-" + orderId));
    }

    @Operation(
        summary = "Cancel the order",
        description = """
                Cancels the order from any non-terminal state.
                
                If a Temporal workflow is active, a **CancelOrder signal** is sent to it.
                Temporal then runs the appropriate saga compensation:
                - If inventory was reserved → releases it
                - If payment was taken → issues a refund
                
                Stored event: `OrderCancelledEvent` (by CUSTOMER).
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Order cancelled"),
        @ApiResponse(responseCode = "400", description = "Order is already in a terminal state (DELIVERED or CANCELLED)"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @PostMapping("/{orderId}/cancel")
    public ResponseEntity<Void> cancelOrder(
            @PathVariable UUID orderId,
            @Valid @RequestBody CancelOrderRequest request) {

// Logging removed
        commandService.cancelOrder(new CancelOrderCommand(OrderId.of(orderId), request.reason()));
        return ResponseEntity.ok().build();
    }

    @Operation(
        summary = "Record a payment (webhook / manual)",
        description = "Records a `PaymentCompletedEvent` directly — used for webhook-based payment flows " +
                      "outside the standard Temporal workflow. In the normal flow, payment is handled " +
                      "automatically by `PaymentActivity`.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Payment recorded"),
        @ApiResponse(responseCode = "400", description = "Missing transactionId or order not in correct state"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @PostMapping("/{orderId}/payment")
    public ResponseEntity<Map<String, String>> recordPayment(
            @PathVariable UUID orderId,
            @RequestBody Map<String, String> body) {

        String transactionId = body.get("transactionId");
        if (transactionId == null || transactionId.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "transactionId is required"));
        }
        commandService.recordPaymentCompleted(OrderId.of(orderId), transactionId);
        return ResponseEntity.ok(Map.of("message", "Payment recorded", "transactionId", transactionId));
    }

    @Operation(
        summary = "Retry payment (send Temporal signal)",
        description = """
                Sends a **RetryPayment signal** to the running Temporal workflow.
                Only effective when the workflow is in `WAITING_FOR_PAYMENT_RETRY` state
                (after a transient payment failure, before the 5-minute timeout).
                
                The workflow resumes and retries the `PaymentActivity`.
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "RetryPayment signal sent to workflow"),
        @ApiResponse(responseCode = "400", description = "Order has no active workflow"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @PostMapping("/{orderId}/retry-payment")
    public ResponseEntity<Map<String, String>> retryPayment(@PathVariable UUID orderId) {
// Logging removed
        Order order = queryService.getOrder(OrderId.of(orderId));
        if (order.getWorkflowId() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "No active workflow for this order"));
        }
        workflowPort.sendRetryPaymentSignal(order.getWorkflowId());
        return ResponseEntity.accepted().body(Map.of("message", "Retry payment signal sent to workflow"));
    }

    // ═══════════════════════════════════════════════════════
    // QUERY ENDPOINTS
    // ═══════════════════════════════════════════════════════

    @Operation(
        summary = "List orders",
        description = """
                Returns a paginated list of orders from the **read model** (order_summary projection).

                Results come from a denormalized projection table updated by `OrderProjectionUpdater`
                on every domain event — no event replay needed. Sub-millisecond queries regardless
                of how many events each order has accumulated.

                **Filters** (all optional, combinable):
                - `status` — comma-separated list: `DRAFT,CONFIRMED,INVENTORY_RESERVED,PAYMENT_COMPLETED,SHIPPED,DELIVERED,CANCELLED`
                - `customerId` — filter to a single customer's orders

                **Pagination**: `page` (0-based) and `size` (default 20, max 100).
                Results are ordered by `createdAt DESC`.
                """)
    @ApiResponse(responseCode = "200", description = "Paginated order list")
    @GetMapping
    public ResponseEntity<Page<OrderSummaryResponse>> listOrders(
            @Parameter(description = "Comma-separated statuses, e.g. DELIVERED,CANCELLED")
            @RequestParam(required = false) String status,
            @Parameter(description = "Filter by customer UUID")
            @RequestParam(required = false) UUID customerId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        List<String> statuses = (status != null && !status.isBlank())
                ? Arrays.asList(status.split(","))
                : null;
        String customerIdStr = customerId != null ? customerId.toString() : null;
        int clampedSize = Math.min(size, 100);

        Page<OrderSummaryResponse> result = queryService.listOrders(
                statuses, customerIdStr, PageRequest.of(page, clampedSize, Sort.by("created_at").descending()));

        return ResponseEntity.ok(result);
    }

    @Operation(
        summary = "Get current order state",
        description = """
                Returns the current state of the order.
                
                **How it works (Event Sourcing):** The order is NOT read from a 'current state' table.
                Instead, all `DomainEvent`s for this orderId are loaded from the `event_store`
                table and replayed in sequence. The final in-memory state is returned.
                
                If a snapshot exists (taken every 50 events), it is loaded first and only
                subsequent events are replayed — bounding replay to at most 50 events.
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Order state"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(
            @Parameter(description = "Order UUID", required = true) @PathVariable UUID orderId) {

        Order order = queryService.getOrder(OrderId.of(orderId));
        return ResponseEntity.ok(orderMapper.toResponse(order));
    }

    @Operation(
        summary = "Get raw event history",
        description = """
                Returns every domain event ever stored for this order, in version order.
                
                This is the **raw event store** — immutable facts, never updated, never deleted.
                Each entry includes: `eventId`, `eventType`, `version`, `occurredAt`, and full `payload`.
                
                Use this for: auditing, debugging, compliance, or understanding exactly what happened and when.
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "List of all domain events"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @GetMapping("/{orderId}/history")
    public ResponseEntity<List<EventHistoryResponse>> getOrderHistory(
            @Parameter(description = "Order UUID", required = true) @PathVariable UUID orderId) {

        List<DomainEvent> events = queryService.getOrderHistory(OrderId.of(orderId));
        return ResponseEntity.ok(orderMapper.toEventHistoryResponses(events));
    }

    @Operation(
        summary = "Get human-readable event timeline",
        description = """
                Returns the same events as `/history` but formatted for human reading.
                Each entry has: `version`, `eventType`, `occurredAt`, and a plain-English `description`.
                
                Example description: *"Payment completed. Transaction: TXN-9051C2C1 Amount: $159.97"*
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Timeline of events"),
        @ApiResponse(responseCode = "404", description = "Order not found")
    })
    @GetMapping("/{orderId}/timeline")
    public ResponseEntity<List<OrderQueryUseCase.TimelineEntry>> getOrderTimeline(
            @Parameter(description = "Order UUID", required = true) @PathVariable UUID orderId) {

        List<OrderQueryUseCase.TimelineEntry> timeline = queryService.getOrderTimeline(OrderId.of(orderId));
        return ResponseEntity.ok(timeline);
    }
}
