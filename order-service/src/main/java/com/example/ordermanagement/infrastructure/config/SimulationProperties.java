package com.example.ordermanagement.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration: SimulationProperties
 *
 * Controls failure rates for simulated external services.
 * These allow testing retry logic and saga compensation paths
 * without needing real external service failures.
 *
 * Configuration via application.yml:
 *   simulation:
 *     inventory-failure-rate: 0.2   # 20%
 *     payment-failure-rate: 0.3     # 30%
 *     shipping-failure-rate: 0.1    # 10%
 *
 * To GUARANTEE a failure path (for demos): set rate to 1.0
 * To GUARANTEE success path: set rate to 0.0
 */
@Component
@ConfigurationProperties(prefix = "simulation")
public class SimulationProperties {

    /** Probability [0.0, 1.0] that inventory reservation will fail */
    private double inventoryFailureRate = 0.2;

    /** Probability [0.0, 1.0] that payment processing will fail */
    private double paymentFailureRate = 0.3;

    /** Probability [0.0, 1.0] that shipment creation will fail */
    private double shippingFailureRate = 0.1;
}
