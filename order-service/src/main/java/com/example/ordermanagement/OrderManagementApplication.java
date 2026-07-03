package com.example.ordermanagement;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * OrderManagementApplication
 *
 * ═══════════════════════════════════════════════════════════════════
 * LEARNING GUIDE: What This Project Demonstrates
 * ═══════════════════════════════════════════════════════════════════
 *
 * 1. TEMPORAL + EVENT SOURCING TOGETHER
 *    ┌────────────────────┐     ┌─────────────────────────────┐
 *    │   Temporal         │     │   Event Sourcing (Domain)   │
 *    │   (Orchestration)  │     │   (State)                   │
 *    ├────────────────────┤     ├─────────────────────────────┤
 *    │ - Workflow steps   │────▶│ - OrderCreatedEvent         │
 *    │ - Retry policies   │     │ - InventoryReservedEvent    │
 *    │ - Signals/Queries  │     │ - PaymentCompletedEvent     │
 *    │ - Compensation     │     │ - OrderCancelledEvent       │
 *    │ - Durability       │     │ - Audit trail               │
 *    └────────────────────┘     └─────────────────────────────┘
 *
 * 2. HEXAGONAL ARCHITECTURE
 *    ┌─────────────────────────────────────────────┐
 *    │                   API Layer                  │
 *    │        (Controllers, DTOs, Mappers)          │
 *    ├─────────────────────────────────────────────┤
 *    │              Application Layer               │
 *    │     (OrderCommandService, QueryService)      │
 *    ├─────────────────────────────────────────────┤
 *    │                Domain Layer                  │
 *    │  (Order aggregate, Events, Commands, Ports)  │
 *    ├─────────────────────────────────────────────┤
 *    │            Infrastructure Layer              │
 *    │  (EventStoreAdapter, WorkflowPortAdapter,    │
 *    │   ActivityImpls, SnapshotStoreAdapter)        │
 *    └─────────────────────────────────────────────┘
 *
 * 3. START HERE for learning:
 *    a. Order.java — understand the aggregate and event sourcing pattern
 *    b. OrderFulfillmentWorkflowImpl.java — understand Temporal orchestration
 *    c. EventStoreAdapter.java — understand event persistence
 *    d. OrderRepositoryAdapter.java — understand replay and snapshotting
 *    e. InventoryActivityImpl.java — understand activities and idempotency
 *
 * TO RUN:
 *   docker-compose up -d
 *   mvn spring-boot:run
 *
 * THEN TRY:
 *   1. POST /orders → create order
 *   2. POST /orders/{id}/items → add items
 *   3. POST /orders/{id}/confirm → start workflow
 *   4. GET /orders/{id}/timeline → watch events accumulate
 *   5. Open http://localhost:8088 → Temporal UI to see workflow execution
 */
@SpringBootApplication
@EnableConfigurationProperties
public class OrderManagementApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderManagementApplication.class, args);
    }
}
