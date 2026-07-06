package com.example.ordermanagement.infrastructure.temporal.activity;

import com.example.contracts.api.ShippingContracts;
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
import org.springframework.web.client.RestClient;

/**
 * ShippingActivityImpl — HTTP client to shipping-service
 *
 * WHAT CHANGED FROM MONOLITH:
 * Before: Generated fake shipmentId/trackingNumber in-process.
 * Now:    Real HTTP call to shipping-service:8083.
 *
 * DELIVERY CONFIRMATION:
 * In production this would wait for a carrier webhook signal.
 * The shipping-service exposes POST /deliveries/{shipmentId}/confirm
 * which simulates the carrier callback.
 */
@Component
@ActivityImpl(taskQueues = "ORDER_FULFILLMENT_QUEUE")
public class ShippingActivityImpl implements ShippingActivity {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ShippingActivityImpl.class);

    private final OrderCommandService orderCommandService;
    private final OrderRepository orderRepository;
    private final RestClient.Builder restClientBuilder;
    private final CircuitBreaker circuitBreaker;

    @Value("${services.shipping.url:http://localhost:8083}")
    private String shippingServiceUrl;

    @Value("${simulation.step-delay-ms:0}")
    private int stepDelayMs;

    public ShippingActivityImpl(OrderCommandService orderCommandService, OrderRepository orderRepository,
                                RestClient.Builder restClientBuilder,
                                CircuitBreakerRegistry circuitBreakerRegistry) {
        this.orderCommandService = orderCommandService;
        this.orderRepository     = orderRepository;
        this.restClientBuilder   = restClientBuilder;
        this.circuitBreaker      = circuitBreakerRegistry.circuitBreaker("shipping-service");
    }

    @Override
    public ShipmentResult createShipment(String orderId) {
        simulateDelay();

        Order order = orderRepository.findById(OrderId.of(orderId))
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        ShippingContracts.CreateShipmentRequest request =
                new ShippingContracts.CreateShipmentRequest(orderId, order.getShippingAddress());

        try {
            ShippingContracts.CreateShipmentResponse response =
                    circuitBreaker.executeSupplier(() -> restClientBuilder.build()
                            .post()
                            .uri(shippingServiceUrl + "/shipments")
                            .body(request)
                            .retrieve()
                            .body(ShippingContracts.CreateShipmentResponse.class));

            log.info("Shipment created for order {}: shipmentId={}, tracking={}, carrier={}",
                    orderId, response.shipmentId(), response.trackingNumber(), response.carrier());
            return new ShipmentResult(response.shipmentId(), response.trackingNumber(), response.carrier());

        } catch (Exception e) {
            log.warn("Shipping service error for order {} (Temporal will retry): {}", orderId, e.getMessage());
            throw Activity.wrap(new RuntimeException("Shipping service error: " + e.getMessage()));
        }
    }

    @Override
    public void confirmDelivery(String orderId, String shipmentId) {
        log.info("Confirming delivery for order {}, shipmentId={}", orderId, shipmentId);
        try {
            circuitBreaker.executeRunnable(() -> restClientBuilder.build()
                    .post()
                    .uri(shippingServiceUrl + "/deliveries/{shipmentId}/confirm", shipmentId)
                    .retrieve()
                    .body(ShippingContracts.ConfirmDeliveryResponse.class));
            log.info("Delivery confirmed for order {}, shipmentId={}", orderId, shipmentId);
        } catch (Exception e) {
            log.error("Delivery confirmation failed for order {}, shipmentId={}: {}", orderId, shipmentId, e.getMessage());
            throw Activity.wrap(new RuntimeException("Delivery confirmation failed: " + e.getMessage()));
        }
    }

    @Override
    public void recordShipmentCreated(String orderId, String shipmentId,
                                       String trackingNumber, String carrier) {
        try {
            orderCommandService.recordShipmentCreated(OrderId.of(orderId), shipmentId, trackingNumber, carrier);
            log.debug("ShipmentCreated recorded for order {}", orderId);
        } catch (DuplicateKeyException | OptimisticLockingException e) {
            // Event already recorded (idempotent)
            log.debug("ShipmentCreated already recorded for order {} — skipping (idempotent)", orderId);
        } catch (Exception e) {
            // Real error — rethrow wrapped for Temporal to handle
            log.error("Failed to record ShipmentCreated for order {}: {}", orderId, e.getMessage());
            throw Activity.wrap(e);
        }
    }

    @Override
    public void recordShipmentDelivered(String orderId, String shipmentId) {
        try {
            orderCommandService.recordOrderDelivered(OrderId.of(orderId), shipmentId);
            log.debug("ShipmentDelivered recorded for order {}", orderId);
        } catch (DuplicateKeyException | OptimisticLockingException e) {
            // Event already recorded (idempotent)
            log.debug("ShipmentDelivered already recorded for order {} — skipping (idempotent)", orderId);
        } catch (Exception e) {
            // Real error — rethrow wrapped for Temporal to handle
            log.error("Failed to record ShipmentDelivered for order {}: {}", orderId, e.getMessage());
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
