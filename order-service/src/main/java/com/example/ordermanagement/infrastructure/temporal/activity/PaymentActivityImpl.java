package com.example.ordermanagement.infrastructure.temporal.activity;

import com.example.contracts.api.PaymentContracts;
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
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

/**
 * PaymentActivityImpl — HTTP client to payment-service
 *
 * WHAT CHANGED FROM MONOLITH:
 * Before: Random failure simulation in-process.
 * Now:    Real HTTP call to payment-service:8082.
 *
 * NON-RETRYABLE EXCEPTIONS:
 * The payment-service returns HTTP 422 for non-retryable failures
 * (insufficient funds, card declined). We translate those to
 * InsufficientFundsException / CardDeclinedException so Temporal's
 * doNotRetry policy kicks in correctly.
 */
@Component
@ActivityImpl(taskQueues = "ORDER_FULFILLMENT_QUEUE")
public class PaymentActivityImpl implements PaymentActivity {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PaymentActivityImpl.class);

    private final OrderCommandService orderCommandService;
    private final OrderRepository orderRepository;
    private final RestClient.Builder restClientBuilder;
    private final CircuitBreaker circuitBreaker;

    @Value("${services.payment.url:http://localhost:8082}")
    private String paymentServiceUrl;

    @Value("${simulation.step-delay-ms:0}")
    private int stepDelayMs;

    public PaymentActivityImpl(OrderCommandService orderCommandService, OrderRepository orderRepository,
                               RestClient.Builder restClientBuilder,
                               CircuitBreakerRegistry circuitBreakerRegistry) {
        this.orderCommandService = orderCommandService;
        this.orderRepository     = orderRepository;
        this.restClientBuilder   = restClientBuilder;
        this.circuitBreaker      = circuitBreakerRegistry.circuitBreaker("payment-service");
    }

    @Override
    public PaymentResult processPayment(String orderId) {
        simulateDelay();

        Order order = orderRepository.findById(OrderId.of(orderId))
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        PaymentContracts.ChargePaymentRequest request = new PaymentContracts.ChargePaymentRequest(
                orderId,
                order.getTotalAmount().amount(),
                order.getTotalAmount().getCurrencyCode());

        try {
            PaymentContracts.ChargePaymentResponse response =
                    circuitBreaker.executeSupplier(() -> restClientBuilder.build()
                            .post()
                            .uri(paymentServiceUrl + "/charge")
                            .body(request)
                            .retrieve()
                            .onStatus(HttpStatusCode::is4xxClientError, (req, resp) -> {
                                String body = new String(resp.getBody().readAllBytes());
                                if (resp.getStatusCode().value() == 422) {
                                    if (body.contains("INSUFFICIENT_FUNDS")) {
                                        throw new InsufficientFundsException("Insufficient funds in account");
                                    } else if (body.contains("CARD_DECLINED")) {
                                        throw new CardDeclinedException("Card declined by issuing bank");
                                    }
                                }
                                throw new RuntimeException("Payment failed: " + body);
                            })
                            .body(PaymentContracts.ChargePaymentResponse.class));

// Logging removed
            return new PaymentResult(response.transactionId(), response.message());

        } catch (InsufficientFundsException | CardDeclinedException e) {
            throw e;  // Let Temporal's doNotRetry handle these
        } catch (Exception e) {
// Logging removed
            throw Activity.wrap(new RuntimeException("Payment service error: " + e.getMessage()));
        }
    }

    @Override
    public void refundPayment(String orderId, String transactionId) {
// Logging removed
        try {
            circuitBreaker.executeRunnable(() -> restClientBuilder.build()
                    .post()
                    .uri(paymentServiceUrl + "/refund")
                    .body(new PaymentContracts.RefundPaymentRequest(orderId, transactionId))
                    .retrieve()
                    .body(PaymentContracts.RefundPaymentResponse.class));
// Logging removed
        } catch (Exception e) {
// Logging removed
            throw Activity.wrap(new RuntimeException("Refund failed: " + e.getMessage()));
        }
    }

    @Override
    public void recordPaymentCompleted(String orderId, String transactionId) {
        try {
            orderCommandService.recordPaymentCompleted(OrderId.of(orderId), transactionId);
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
    public void recordPaymentFailed(String orderId, String reason, boolean retryable) {
        try {
            orderCommandService.recordPaymentFailed(OrderId.of(orderId), reason, retryable);
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
    public void recordRefundCompleted(String orderId, String refundTransactionId) {
        try {
            orderCommandService.recordRefundCompleted(OrderId.of(orderId), refundTransactionId);
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
