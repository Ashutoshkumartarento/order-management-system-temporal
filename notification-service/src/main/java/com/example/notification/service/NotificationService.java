package com.example.notification.service;

import com.example.contracts.kafka.OrderEventMessage;
import com.example.contracts.kafka.PaymentEventMessage;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * NotificationService — core business logic for sending notifications.
 *
 * In reality, this would call:
 *   - Email service (AWS SES, SendGrid, Mailgun)
 *   - SMS service (Twilio, AWS SNS)
 *   - Push notification service
 *   - In-app notification database
 *
 * For this PoC: simulates sending by logging.
 * Idempotency is handled by the Kafka consumer (deduplication by eventId).
 */
@Service
public class NotificationService {

    /**
     * Send welcome email when order is created.
     */
    public void notifyOrderCreated(OrderEventMessage.OrderCreatedMessage event) {
        sendEmail(
            event.orderId(),
            "Order Created",
            String.format("""
                Welcome! Your order has been created.
                Order ID: %s
                Shipping Address: %s
                """, event.orderId(), event.shippingAddress()));
    }

    /**
     * Send confirmation email when order is confirmed and workflow starts.
     */
    public void notifyOrderConfirmed(OrderEventMessage.OrderConfirmedMessage event) {
        sendEmail(
            event.orderId(),
            "Order Confirmed",
            String.format("""
                Your order has been confirmed!
                Order ID: %s
                Total: $%.2f
                Workflow ID: %s
                Your items will be processed shortly.
                """, event.orderId(), event.totalAmount(), event.workflowId()));
    }

    /**
     * Send cancellation email when order is cancelled.
     */
    public void notifyOrderCancelled(OrderEventMessage.OrderCancelledMessage event) {
        sendEmail(
            event.orderId(),
            "Order Cancelled",
            String.format("""
                Your order has been cancelled.
                Order ID: %s
                Reason: %s
                Cancelled By: %s
                """, event.orderId(), event.reason(), event.cancelledBy()));
    }

    /**
     * Send receipt email when payment completes.
     */
    public void notifyPaymentCompleted(OrderEventMessage.PaymentCompletedMessage event) {
        sendEmail(
            event.orderId(),
            "Payment Receipt",
            String.format("""
                Payment confirmed!
                Order ID: %s
                Amount: $%.2f
                Transaction ID: %s
                """, event.orderId(), event.amount(), event.transactionId()));
    }

    /**
     * Send alert email when payment fails.
     */
    public void notifyPaymentFailed(OrderEventMessage.PaymentFailedMessage event) {
        sendEmail(
            event.orderId(),
            "Payment Failed - Action Required",
            String.format("""
                We were unable to process your payment.
                Order ID: %s
                Reason: %s
                Retryable: %s
                
                Please update your payment method.
                """, event.orderId(), event.reason(), event.retryable()));
    }

    /**
     * Send tracking email when shipment is created.
     */
    public void notifyShipmentCreated(OrderEventMessage.ShipmentCreatedMessage event) {
        sendEmail(
            event.orderId(),
            "Your Package is on the Way!",
            String.format("""
                Your order is being shipped!
                Order ID: %s
                Shipment ID: %s
                Tracking Number: %s
                Carrier: %s
                
                Track your package: https://carrier.example.com/track?tracking=%s
                """, event.orderId(), event.shipmentId(), event.trackingNumber(), 
                    event.carrier(), event.trackingNumber()));
    }

    /**
     * Send delivery confirmation email.
     */
    public void notifyShipmentDelivered(OrderEventMessage.ShipmentDeliveredMessage event) {
        sendEmail(
            event.orderId(),
            "Your Package Has Arrived!",
            String.format("""
                Your order has been delivered!
                Order ID: %s
                Shipment ID: %s
                Delivered At: %s
                
                Thank you for your order. We hope you enjoy your purchase!
                """, event.orderId(), event.shipmentId(), event.deliveredAt()));
    }

    /**
     * Send receipt when payment-service confirms a charge (from payment.events).
     */
    public void notifyPaymentCharged(PaymentEventMessage.PaymentChargedMessage event) {
        sendEmail(
            event.orderId(),
            "Payment Confirmed",
            String.format("""
                Payment confirmed!
                Order ID: %s
                Amount: %s %s
                Transaction ID: %s
                """, event.orderId(), event.amount(), event.currency(), event.transactionId()));
    }

    /**
     * Send alert when a payment charge fails (from payment.events).
     */
    public void notifyPaymentChargeFailed(PaymentEventMessage.PaymentFailedMessage event) {
        sendEmail(
            event.orderId(),
            "Payment Failed - Action Required",
            String.format("""
                We were unable to process your payment.
                Order ID: %s
                Reason: %s

                Please update your payment method or contact support.
                """, event.orderId(), event.reason()));
    }

    /**
     * Send refund confirmation (from payment.events — not published on order.events).
     */
    public void notifyPaymentRefunded(PaymentEventMessage.PaymentRefundedMessage event) {
        sendEmail(
            event.orderId(),
            "Refund Processed",
            String.format("""
                Your refund has been processed.
                Order ID: %s
                Refund ID: %s
                Amount: %s
                Original Transaction: %s

                Funds will appear in your account within 3-5 business days.
                """, event.orderId(), event.refundTransactionId(),
                    event.amount(), event.originalTransactionId()));
    }

    /**
     * Send SMS alert for order status changes (optional).
     */
    public void sendSms(String orderId, String phone, String message) {
        // In reality: call Twilio API or AWS SNS
    }

    /**
     * Send email notification.
     * In reality: call SendGrid, AWS SES, or similar.
     */
    private void sendEmail(String orderId, String subject, String body) {
        // In reality: call email service API
        // emailClient.send(Email.to(customer.email).subject(subject).body(body));
    }

    /**
     * Persist notification to database for audit trail / resend support.
     */
    private void persistNotification(String orderId, String type, String recipient, String message) {
        Map<String, Object> notification = new HashMap<>();
        notification.put("orderId", orderId);
        notification.put("type", type);
        notification.put("recipient", recipient);
        notification.put("message", message);
        notification.put("sentAt", System.currentTimeMillis());
        
        // In reality: save to notifications table
    }
}
