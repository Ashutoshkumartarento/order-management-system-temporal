package com.example.ordermanagement.config;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration: TemporalConfig
 *
 * ═══════════════════════════════════════════════════════════════════
 * TEMPORAL CONNECTION SETUP
 * ═══════════════════════════════════════════════════════════════════
 *
 * WorkflowServiceStubs: Low-level gRPC connection to Temporal server.
 *   Think of this as the JDBC DataSource equivalent for Temporal.
 *
 * WorkflowClient: High-level client for starting workflows, sending signals,
 *   and running queries. Uses WorkflowServiceStubs under the hood.
 *
 * Note: We're using Temporal's Spring Boot auto-configuration (@ActivityImpl)
 * for worker registration. This auto-configures the worker with all beans
 * annotated with @ActivityImpl and all @WorkflowInterface implementations.
 *
 * The WorkflowClient bean is used by WorkflowPortAdapter to start workflows
 * and interact with running workflow instances.
 */
@Configuration
public class TemporalConfig {

    @Value("${temporal.service-address:localhost:7233}")
    private String temporalServiceAddress;

    @Value("${temporal.namespace:default}")
    private String namespace;

    /**
     * gRPC stub to the Temporal server.
     * In production: use TLS, health checks, and connection pooling.
     */
    @Bean
    public WorkflowServiceStubs workflowServiceStubs() {
        return WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder()
                        .setTarget(temporalServiceAddress)
                        .build()
        );
    }

    /**
     * Temporal WorkflowClient.
     * Namespace defaults to "default" — in production, use separate namespaces
     * per environment (dev, staging, prod) for isolation.
     */
    @Bean
    public WorkflowClient workflowClient(WorkflowServiceStubs stubs) {
        return WorkflowClient.newInstance(stubs,
                WorkflowClientOptions.newBuilder()
                        .setNamespace(namespace)
                        .build()
        );
    }

    /**
     * WorkerFactory creates and manages Temporal workers.
     * Workers are the processes that execute workflow and activity code.
     * The Spring Boot Temporal integration auto-configures workers from
     * @ActivityImpl and WorkflowInterface implementations.
     */
    @Bean
    public WorkerFactory workerFactory(WorkflowClient workflowClient) {
        return WorkerFactory.newInstance(workflowClient);
    }
}
