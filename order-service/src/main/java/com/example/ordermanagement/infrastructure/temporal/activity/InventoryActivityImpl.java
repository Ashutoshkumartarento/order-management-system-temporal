package com.example.ordermanagement.infrastructure.temporal.activity;

import com.example.contracts.api.InventoryContracts;
import com.example.ordermanagement.application.service.OrderCommandService;
import com.example.ordermanagement.domain.aggregate.Order;
import com.example.ordermanagement.domain.exception.OrderNotFoundException;
import com.example.ordermanagement.domain.exception.OptimisticLockingException;
import com.example.ordermanagement.domain.port.outbound.OrderRepository;
import com.example.ordermanagement.domain.valueobject.OrderId;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.temporal.activity.Activity;
import io.temporal.spring.boot.ActivityImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.List;

/**
 * InventoryActivityImpl — HTTP client to inventory-service
 *
 * WHAT CHANGED FROM MONOLITH:
 * Before: Simulated success/failure with random numbers in-process.
 * Now:    Makes real HTTP calls to inventory-service:8081.
 *
 * The inventory-service itself controls the failure simulation.
 * This service is just a client — it calls, parses the response,
 * and records the result as a domain event.
 *
 * RETRY BEHAVIOUR:
 * Temporal handles retries when this throws an exception.
 * The inventory-service must be idempotent for the same reservationId
 * (which we pass as the orderId — deterministic per order).
 *
 * CIRCUIT BREAKER:
 * In production, wrap RestClient calls with Resilience4j circuit breaker.
 * Not added here to keep the demo focused on Temporal + Event Sourcing.
 */
@Component
@ActivityImpl(taskQueues = "ORDER_FULFILLMENT_QUEUE")
public class InventoryActivityImpl implements InventoryActivity {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(InventoryActivityImpl.class);

    private final OrderCommandService orderCommandService;
    private final OrderRepository orderRepository;
    private final RestClient.Builder restClientBuilder;
    private final CircuitBreaker circuitBreaker;

    @Value("${services.inventory.url:http://localhost:8081}")
    private String inventoryServiceUrl;

    @Value("${simulation.step-delay-ms:0}")
    private int stepDelayMs;

    public InventoryActivityImpl(OrderCommandService orderCommandService, OrderRepository orderRepository,
                                 RestClient.Builder restClientBuilder,
                                 CircuitBreakerRegistry circuitBreakerRegistry) {
        this.orderCommandService = orderCommandService;
        this.orderRepository     = orderRepository;
        this.restClientBuilder   = restClientBuilder;
        this.circuitBreaker      = circuitBreakerRegistry.circuitBreaker("inventory-service");
    }

    @Override
    public ReservationResult reserveInventory(String orderId) {
        simulateDelay();

        // Load order items to know what to reserve
        Order order = orderRepository.findById(OrderId.of(orderId))
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        List<InventoryContracts.ReserveInventoryRequest.LineItem> lineItems = order.getItems().stream()
                .map(item -> new InventoryContracts.ReserveInventoryRequest.LineItem(
                        item.productId(), item.quantity()))
                .toList();

        InventoryContracts.ReserveInventoryRequest request =
                new InventoryContracts.ReserveInventoryRequest(orderId, lineItems);

        try {
            InventoryContracts.ReserveInventoryResponse response =
                    circuitBreaker.executeSupplier(() -> restClientBuilder.build()
                            .post()
                            .uri(inventoryServiceUrl + "/reserve")
                            .body(request)
                            .retrieve()
                            .body(InventoryContracts.ReserveInventoryResponse.class));

// Logging removed
            return new ReservationResult(response.reservationId(), response.message());

        } catch (HttpClientErrorException.UnprocessableEntity e) {
            // 422 from inventory-service = not enough stock (non-retryable)
// Logging removed
            throw Activity.wrap(new RuntimeException("Insufficient stock: " + e.getMessage()));
        } catch (Exception e) {
            // Network errors, 5xx, or circuit open = retryable by Temporal
// Logging removed
            throw Activity.wrap(new RuntimeException("Inventory service unavailable: " + e.getMessage()));
        }
    }

    @Override
    public void releaseInventory(String orderId, String reservationId) {
// Logging removed
        try {
            circuitBreaker.executeRunnable(() -> restClientBuilder.build()
                    .delete()
                    .uri(inventoryServiceUrl + "/reserve/{reservationId}", reservationId)
                    .retrieve()
                    .toBodilessEntity());
// Logging removed
        } catch (Exception e) {
// Logging removed
            throw Activity.wrap(new RuntimeException("Failed to release inventory: " + e.getMessage()));
        }
    }

    @Override
    public void recordInventoryReserved(String orderId, String reservationId) {
        try {
            orderCommandService.recordInventoryReserved(OrderId.of(orderId), reservationId);
// Logging removed
        } catch (DuplicateKeyException | OptimisticLockingException e) {
            // Event already recorded (idempotent)
// Logging removed
        } catch (Exception e) {
            // Real error — rethrow wrapped for Temporal to handle
// Logging removed
            throw Activity.wrap(e);
        }
    }

    @Override
    public void recordInventoryReservationFailed(String orderId, String reason) {
        try {
            orderCommandService.recordInventoryReservationFailed(OrderId.of(orderId), reason);
// Logging removed
        } catch (DuplicateKeyException | OptimisticLockingException e) {
            // Event already recorded (idempotent)
// Logging removed
        } catch (Exception e) {
            // Real error — rethrow wrapped for Temporal to handle
// Logging removed
            throw Activity.wrap(e);
        }
    }

    @Override
    public void recordInventoryReleased(String orderId, String reason) {
        try {
            orderCommandService.recordInventoryReleased(OrderId.of(orderId), reason);
// Logging removed
        } catch (DuplicateKeyException | OptimisticLockingException e) {
            // Event already recorded (idempotent)
// Logging removed
        } catch (Exception e) {
            // Real error — rethrow wrapped for Temporal to handle
// Logging removed
            throw Activity.wrap(e);
        }
    }

    @Override
    public void recordOrderCancelled(String orderId, String reason, String cancelledBy) {
        try {
            orderCommandService.recordOrderCancelled(OrderId.of(orderId), reason, cancelledBy);
// Logging removed
        } catch (DuplicateKeyException | OptimisticLockingException e) {
            // Event already recorded (idempotent)
// Logging removed
        } catch (Exception e) {
            // Real error — rethrow wrapped for Temporal to handle
// Logging removed
            throw Activity.wrap(e);
        }
    }

    private void simulateDelay() {
        if (stepDelayMs <= 0) return;
        try {
            Thread.sleep(stepDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
