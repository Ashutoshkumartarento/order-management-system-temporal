package com.example.ordermanagement.infrastructure.temporal.activity;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity Interface: NotificationActivity
 *
 * Sends notifications to customers at various workflow stages.
 * Notifications are best-effort — failures don't abort the workflow.
 */
@ActivityInterface
public interface NotificationActivity {

    @ActivityMethod
    void sendOrderConfirmedNotification(String orderId);

    @ActivityMethod
    void sendOrderShippedNotification(String orderId, String trackingNumber);

    @ActivityMethod
    void sendOrderDeliveredNotification(String orderId);

    @ActivityMethod
    void sendOrderCancelledNotification(String orderId, String reason);

    @ActivityMethod
    void sendPaymentFailedNotification(String orderId, String reason);
}
