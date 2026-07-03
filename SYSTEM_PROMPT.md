# System Prompt — Order Management System

Use this prompt to give any AI model full context to understand, extend, or rebuild this application.

---

## What You Are Building

A **production-quality Order Management System** that demonstrates four enterprise patterns working together:

1. **Event Sourcing** — Every state change is an immutable domain event. State is never stored directly; it is rebuilt by replaying events.
2. **Temporal Workflows** — A durable Saga orchestrates the fulfillment lifecycle (inventory → payment → shipping) with automatic retries, crash recovery, and compensation.
3. **Hexagonal Architecture (Ports and Adapters)** — The domain has zero dependencies on Spring, Temporal, or any database. All external concerns plug in through interfaces.
4. **CQRS** — Commands and queries are handled by separate services, never mixed.

**Tech stack:** Java 21, Spring Boot 3.2.x, Temporal Java SDK 1.25.2, Spring Data JDBC, PostgreSQL 16, Flyway, Apache Kafka, Kong API Gateway, MapStruct, springdoc-openapi 2.5.0.

**Package root:** `com.example.ordermanagement`

---

## Architecture: Strict Dependency Direction

```
API Layer  →  Application Layer  →  Domain Layer  ←  Infrastructure Layer
```

- **Domain Layer** — zero dependencies on Spring, Temporal, Kafka, or any framework. Only Java 21 stdlib and Jackson annotations for serialization.
- **Application Layer** — depends only on Domain. Coordinates domain objects and calls outbound ports (interfaces).
- **Infrastructure Layer** — implements domain ports. Knows about Spring, Temporal SDK, JDBC, Kafka.
- **API Layer** — depends only on Application. Controllers call application services only; never repositories or aggregates directly.

**Never violate this direction.** Business logic never goes in controllers. Repositories never contain business logic.

---

## Project Structure

```
order-service/src/main/java/com/example/ordermanagement/
│
├── api/
│   ├── controller/
│   │   ├── OrderController.java          POST/GET/DELETE endpoints for orders
│   │   ├── WorkflowController.java       Temporal signal/query endpoints
│   │   └── GlobalExceptionHandler.java   RFC 7807 ProblemDetail error responses
│   ├── dto/request/
│   │   ├── CreateOrderRequest.java
│   │   ├── AddItemRequest.java
│   │   └── CancelOrderRequest.java
│   ├── dto/response/
│   │   ├── OrderResponse.java
│   │   └── EventHistoryResponse.java
│   └── mapper/OrderMapper.java           MapStruct: domain ↔ DTO
│
├── application/service/
│   ├── OrderCommandService.java          All writes (@Transactional)
│   └── OrderQueryService.java            All reads (@Transactional(readOnly=true))
│
├── domain/
│   ├── aggregate/
│   │   ├── Order.java                    Aggregate root (event sourcing)
│   │   └── OrderSnapshot.java            Snapshot at every 50 events
│   ├── command/
│   │   ├── CreateOrderCommand.java
│   │   ├── AddItemCommand.java
│   │   ├── RemoveItemCommand.java
│   │   ├── ConfirmOrderCommand.java
│   │   └── CancelOrderCommand.java
│   ├── event/
│   │   ├── DomainEvent.java              Sealed interface (13 permitted subtypes)
│   │   ├── OrderCreatedEvent.java
│   │   ├── ItemAddedEvent.java
│   │   ├── ItemRemovedEvent.java
│   │   ├── OrderConfirmedEvent.java
│   │   ├── InventoryReservedEvent.java
│   │   ├── InventoryReservationFailedEvent.java
│   │   ├── InventoryReleasedEvent.java
│   │   ├── PaymentCompletedEvent.java
│   │   ├── PaymentFailedEvent.java
│   │   ├── RefundCompletedEvent.java
│   │   ├── ShipmentCreatedEvent.java
│   │   ├── ShipmentDeliveredEvent.java
│   │   └── OrderCancelledEvent.java
│   ├── exception/
│   │   ├── DomainException.java
│   │   ├── InvalidStateTransitionException.java
│   │   ├── OptimisticLockingException.java
│   │   └── OrderNotFoundException.java
│   ├── model/
│   │   ├── OrderStatus.java              DRAFT→CONFIRMED→INVENTORY_RESERVED→PAYMENT_COMPLETED→SHIPPED→DELIVERED|CANCELLED
│   │   ├── PaymentStatus.java            PENDING→COMPLETED|FAILED|REFUNDED
│   │   └── ShipmentStatus.java           NOT_CREATED→CREATED→DELIVERED
│   ├── port/outbound/
│   │   ├── EventStore.java               append / load by aggregateId
│   │   ├── SnapshotStore.java            save / findLatest
│   │   ├── OrderRepository.java          save / findById
│   │   └── WorkflowPort.java             startFulfillment / sendSignal / query
│   └── valueobject/
│       ├── OrderId.java                  Wraps UUID, no raw UUIDs in domain
│       ├── CustomerId.java
│       ├── Money.java                    BigDecimal, never double/float
│       └── OrderItem.java               productId, productName, quantity, unitPrice
│
└── infrastructure/
    ├── config/
    │   └── SimulationProperties.java     Configurable failure rates (YAML-bound)
    ├── observability/
    │   └── CorrelationIdFilter.java      X-Correlation-ID → MDC
    ├── persistence/
    │   ├── EventStoreAdapter.java        Implements EventStore — append-only JSONB
    │   ├── SnapshotStoreAdapter.java     Implements SnapshotStore
    │   └── OrderRepositoryAdapter.java   Implements OrderRepository — snapshot+replay
    ├── kafka/
    │   └── OrderEventKafkaPublisher.java @TransactionalEventListener(AFTER_COMMIT)
    └── temporal/
        ├── WorkflowPortAdapter.java      Implements WorkflowPort — Temporal SDK calls
        ├── worker/TemporalWorkerSetup.java
        ├── activity/
        │   ├── InventoryActivity.java    (interface)
        │   ├── InventoryActivityImpl.java
        │   ├── PaymentActivity.java
        │   ├── PaymentActivityImpl.java
        │   ├── ShippingActivity.java
        │   ├── ShippingActivityImpl.java
        │   ├── NotificationActivity.java
        │   ├── NotificationActivityImpl.java
        │   ├── InsufficientFundsException.java  (non-retryable)
        │   └── CardDeclinedException.java       (non-retryable)
        └── workflow/
            ├── OrderFulfillmentWorkflow.java    (interface: @WorkflowMethod, @SignalMethod, @QueryMethod)
            └── OrderFulfillmentWorkflowImpl.java (saga implementation)
```

---

## Domain: Event Sourcing Rules (Non-Negotiable)

### The Aggregate Contract

The `Order` aggregate is the single source of truth. These rules are absolute:

1. **State is never mutated directly.** Every change goes through a domain event.
2. **Commands raise events. Events change state.** The `apply(DomainEvent)` method is the ONLY place fields are mutated.
3. **`apply()` methods are side-effect free.** No DB calls, no HTTP calls, no logging inside them.
4. **Events are immutable records.** No setters, ever.
5. **`pendingEvents` is drained by the repository, not the service.** Call `order.save()` — the repository handles draining.

```java
// CORRECT pattern:
public void addItem(OrderItem item) {
    requireStatus(OrderStatus.DRAFT, "add items to");   // validate
    ItemAddedEvent event = ItemAddedEvent.create(id, item, version + 1);
    applyAndRecord(event);                               // raise event
}

private void applyItemAdded(ItemAddedEvent event) {     // apply = only mutation
    this.items.add(event.item());
    this.totalAmount = calculateTotal();
}

// WRONG: never do this
public void addItem(OrderItem item) {
    this.items.add(item);  // ❌ direct mutation bypasses event sourcing
}
```

### Adding a New Domain Event — All 6 Steps Together

When adding a new event, all of these must be done as a unit or it will not compile:

1. Create a new `record` in `domain/event/` implementing `DomainEvent`
2. Add it to the `permits` clause of the `DomainEvent` sealed interface
3. Add a `@JsonSubTypes.Type` entry to `DomainEvent`
4. Add `case NewEvent e -> applyNewEvent(e)` in `Order.apply()` (compiler fails if missing)
5. Implement `private void applyNewEvent(NewEvent e)` on `Order`
6. Add the command handler method on `Order` that raises it

### Event Store Rules

- The `event_store` table is **append-only**. No UPDATE or DELETE SQL ever.
- `UNIQUE(aggregate_id, version)` enforces optimistic locking at the DB level.
- Events are stored as JSONB. `@JsonTypeInfo` type discriminator enables polymorphic deserialization.
- Loading order: check for snapshot → load events after snapshot version → replay.
- Snapshot threshold = 50 events. Snapshots are expendable (can be deleted and regenerated).

---

## Temporal Workflow: Determinism Rules (Critical)

Temporal replays workflow history on crash recovery. If the code is non-deterministic, replay diverges from actual history → corruption.

| ❌ NEVER use | ✅ Use instead |
|-------------|--------------|
| `System.currentTimeMillis()` | `Workflow.currentTimeMillis()` |
| `UUID.randomUUID()` | `Workflow.newRandom().nextLong()` |
| `new Random()` | `Workflow.newRandom()` |
| `Thread.sleep()` | `Workflow.sleep(Duration)` |
| Direct HTTP/DB calls | Activity stubs |
| `System.getenv()` | Pass as workflow parameters |
| `CompletableFuture`, `synchronized` | `Workflow.async()`, `Promise` |

### Saga Flow in OrderFulfillmentWorkflowImpl

```
fulfill(orderId)
│
├─ [cancel check] → compensateAndCancel()
│
├─ STEP 1: inventoryActivity.reserveInventory(orderId)
│   SUCCESS → store reservationId, recordInventoryReserved()
│   FAILURE → recordInventoryReservationFailed(), cancelOrderDirectly()
│
├─ [cancel check] → releaseInventory + cancelOrderDirectly
│
├─ STEP 2: paymentActivity.processPayment(orderId)
│   SUCCESS → store transactionId, recordPaymentCompleted()
│   TRANSIENT FAIL → Workflow.await(5min, () -> paymentRetryRequested || cancellationRequested)
│   NON-RETRYABLE (InsufficientFunds/CardDeclined) → compensateInventory + cancelOrderDirectly
│   RETRY EXHAUSTED → compensateInventory + cancelOrderDirectly
│
├─ [cancel check] → refundPayment + releaseInventory + cancelOrderDirectly
│
├─ STEP 3: shippingActivity.createShipment(orderId)
│   SUCCESS → store shipmentId, recordShipmentCreated()
│   FAILURE → compensatePayment + compensateInventory + cancelOrderDirectly
│
├─ STEP 4: shippingActivity.confirmDelivery(orderId, shipmentId)
│           shippingActivity.recordShipmentDelivered(orderId, shipmentId)
│
└─ STEP 5: notificationActivity.sendOrderDeliveredNotification(orderId)
           status = COMPLETED
```

### Signals and Queries

```java
@SignalMethod void cancelOrder(String reason);    // sets cancellationRequested = true
@SignalMethod void retryPayment();                // sets paymentRetryRequested = true

@QueryMethod String getCurrentStatus();           // returns status string
@QueryMethod WorkflowProgress getProgress();      // status, currentStep, retryCount, failureReason
```

Signal handlers ONLY set primitive flags. They NEVER call activities.
Query handlers are ONLY read-only. They NEVER modify state.

### Activity Configuration

```java
// Inventory: 3 retries, 2x backoff
ActivityOptions.newBuilder()
    .setStartToCloseTimeout(Duration.ofSeconds(30))
    .setRetryOptions(RetryOptions.newBuilder()
        .setMaximumAttempts(3).setBackoffCoefficient(2.0).build())
    .build()

// Payment: 2 retries, do-not-retry for InsufficientFunds/CardDeclined
// Use simple class name (not fully qualified) — Temporal uses simple names for type matching
.setDoNotRetry("InsufficientFundsException", "CardDeclinedException")

// Notifications: 2 retries (best-effort, fire-and-forget)
```

### Saga Compensation Table

| Forward Step | Compensation |
|---|---|
| `reserveInventory()` | `releaseInventory()` |
| `processPayment()` | `refundPayment()` |
| `createShipment()` | *(none — no side effect to undo)* |

Compensation always runs in reverse order. Compensation failures are logged but do not abort the chain.

---

## API Endpoints

| Method | Path | Response | Description |
|--------|------|----------|-------------|
| `POST` | `/orders` | 201 + `{orderId}` + Location | Create order in DRAFT |
| `POST` | `/orders/{id}/items` | 200 | Add item (DRAFT only) |
| `DELETE` | `/orders/{id}/items/{productId}` | 200 | Remove item (DRAFT only) |
| `POST` | `/orders/{id}/confirm` | 202 + `{workflowId}` | Confirm → start Temporal saga |
| `POST` | `/orders/{id}/cancel` | 200 | Cancel → send signal |
| `POST` | `/orders/{id}/payment` | 200 | Record payment (activity callback) |
| `POST` | `/orders/{id}/retry-payment` | 202 | Send retry signal |
| `GET` | `/orders/{id}` | 200 + OrderResponse | Current state (event replay) |
| `GET` | `/orders/{id}/history` | 200 + events[] | Raw event store records |
| `GET` | `/orders/{id}/timeline` | 200 + timeline[] | Human-readable event history |
| `GET` | `/workflows/{workflowId}` | 200 + WorkflowProgress | Live Temporal query (no DB) |
| `POST` | `/workflows/{id}/signal/cancel` | 200 | Direct cancel signal |
| `POST` | `/workflows/{id}/signal/retry-payment` | 202 | Retry payment signal |

**API rules:**
- Controllers use Bean Validation (`@Valid`, `@NotNull`). Never validate in services.
- All error responses use RFC 7807 `ProblemDetail` via `GlobalExceptionHandler`.
- `202 Accepted` for async operations. `201 Created` with `Location` for resource creation.
- Every controller has `@Tag`. Every endpoint has `@Operation` and `@ApiResponses`.

---

## Database Schema

### event_store table
```sql
CREATE TABLE event_store (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID        NOT NULL UNIQUE,           -- idempotency key
    aggregate_id  UUID        NOT NULL,
    event_type    VARCHAR(100) NOT NULL,
    version       BIGINT      NOT NULL,
    payload       JSONB       NOT NULL,                  -- serialized event
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (aggregate_id, version)                        -- optimistic locking
);
CREATE INDEX idx_event_store_aggregate_id ON event_store(aggregate_id);
```

### order_snapshots table
```sql
CREATE TABLE order_snapshots (
    aggregate_id  UUID        PRIMARY KEY,
    version       BIGINT      NOT NULL,
    snapshot_data JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Database conventions:**
- All timestamps use `TIMESTAMPTZ`. Never `TIMESTAMP WITHOUT TIME ZONE`.
- Event payloads use `JSONB`, not `TEXT`. Enables indexing.
- Flyway naming: `V{N}__{description}.sql` (two underscores). Never modify existing migrations.
- Foreign keys between microservice databases are forbidden. Reference by ID only.

---

## Microservices (Current State: Implemented)

### Services and Ports

| Service | Port | Database | Failure Rate |
|---------|------|----------|--------------|
| order-service | 8080 | orderdb (5432) | — |
| inventory-service | 8081 | inventorydb (5434) | 20% |
| payment-service | 8082 | paymentdb (5435) | 30% |
| shipping-service | 8083 | shippingdb (5436) | 10% |
| notification-service | 8084 | — (Kafka consumer) | — |

### Inventory Service Endpoints
```
POST   /inventory/reserve              → ReserveInventoryResponse {reservationId, status}
DELETE /inventory/reserve/{reservationId} → ReleaseInventoryResponse
```

### Payment Service Endpoints
```
POST   /payments/charge                → ChargePaymentResponse {transactionId, status}
POST   /payments/refund                → RefundPaymentResponse
```

### Shipping Service Endpoints
```
POST   /shipping/shipments             → CreateShipmentResponse {shipmentId, trackingNumber, carrier}
POST   /shipping/deliveries/{id}/confirm → ConfirmDeliveryResponse
```

### Notification Service
- Kafka consumer on topic `order.events`, group `notification-service-group`
- Dispatches to `NotificationService` based on event type
- Idempotency: deduplicates by `eventId`
- Error handling: logs but never rethrows (best-effort)

### Kafka Rules (Outbox Pattern)
- Persist event to DB first, publish to Kafka in `@TransactionalEventListener(AFTER_COMMIT)`
- Message key MUST be `orderId` (guarantees ordering per partition)
- Every consumer MUST be idempotent — deduplicate by `eventId` before processing
- Consumer group IDs: `{service-name}-group`
- Topics: `order.events`, `inventory.events`, `payment.events`, `shipping.events`, `notification.requests`

### Kong API Gateway
- Client traffic routes through Kong (port 8000)
- Service-to-service traffic (Temporal activity HTTP calls) bypasses Kong
- All Kong routes have `rate-limiting` plugin
- `correlation-id` plugin applied globally — no per-service filter once Kong is active

---

## CQRS Rules

```java
@Service
@Transactional
public class OrderCommandService {  // handles all writes, returns void or new ID only
    public UUID createOrder(CreateOrderRequest req) { ... }
    public void addItem(UUID orderId, AddItemRequest req) { ... }
    public String confirmOrder(UUID orderId) { ... }  // returns workflowId
    // never returns aggregate state
}

@Service
@Transactional(readOnly = true)
public class OrderQueryService {  // handles all reads, never modifies state
    public OrderResponse getOrder(UUID orderId) { ... }
    public List<TimelineEntry> getTimeline(UUID orderId) { ... }
    // never accepts command objects as parameters
}
```

---

## Value Objects and Domain Types

```java
// All value objects are Java records (immutable by definition)
record OrderId(UUID value) { public static OrderId of(UUID v) { return new OrderId(v); } }
record CustomerId(UUID value) { ... }
record Money(BigDecimal amount, String currency) {
    // Never use double or float for financial amounts
    public static final Money ZERO = new Money(BigDecimal.ZERO, "USD");
    public Money add(Money other) { return new Money(amount.add(other.amount), currency); }
    // Returns new instance; never mutates in place
}
record OrderItem(UUID productId, String productName, int quantity, Money unitPrice) {
    public Money totalPrice() { return unitPrice.multiply(quantity); }
}
```

---

## Naming Conventions

| Artifact | Pattern | Example |
|---|---|---|
| Domain events | `{Entity}{PastTense}Event` | `OrderConfirmedEvent` |
| Commands | `{Verb}{Entity}Command` | `ConfirmOrderCommand` |
| Repository ports | `{Entity}Repository` | `OrderRepository` |
| Repository adapters | `{Entity}RepositoryAdapter` | `OrderRepositoryAdapter` |
| Activity interfaces | `{Domain}Activity` | `InventoryActivity` |
| Activity impls | `{Domain}ActivityImpl` | `InventoryActivityImpl` |
| Application services | `Order{Command|Query}Service` | `OrderCommandService` |
| DTOs | `{Entity}{Request|Response}` | `CreateOrderRequest` |
| Kafka topics | `{domain}.events` | `order.events` |

---

## Testing Conventions

| Test type | Suffix | Infrastructure | Speed |
|---|---|---|---|
| Domain unit | `*Test` | None | <1s |
| Application unit | `*Test` | Mockito | <1s |
| Temporal workflow | `*Test` | `TestWorkflowEnvironment` | ~5s |
| Integration | `*IntegrationTest` | Testcontainers PostgreSQL | ~30s |

- Use `TestWorkflowEnvironment` for all Temporal tests. Never require a running Temporal server.
- Activity test doubles MUST be plain POJOs implementing the activity interface. Do NOT use Mockito for Temporal activities — Temporal inspects `@ActivityMethod` annotations and fails on proxies.
- Integration tests use `@DynamicPropertySource` with Testcontainers to inject the real DB URL.

---

## Running the System

### Infrastructure (Docker Compose)
```bash
cd docker
docker compose up -d postgres temporal-postgres temporal temporal-ui kafka zookeeper
# Wait ~20s for Temporal to initialize
```

### Start Services
```bash
# Each in its own terminal
cd order-service      && mvn spring-boot:run
cd inventory-service  && mvn spring-boot:run
cd payment-service    && mvn spring-boot:run
cd shipping-service   && mvn spring-boot:run
cd notification-service && mvn spring-boot:run
```

### Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/orderdb` | Event store DB |
| `TEMPORAL_SERVICE_ADDRESS` | `localhost:7233` | Temporal gRPC |
| `SIMULATION_PAYMENT_FAILURE_RATE` | `0.30` | Payment failure probability |
| `SIMULATION_INVENTORY_FAILURE_RATE` | `0.20` | Inventory failure probability |
| `SIMULATION_SHIPPING_FAILURE_RATE` | `0.10` | Shipping failure probability |

### Testing Failure Paths
```bash
# Always fail payment → demonstrates saga compensation
--simulation.payment-failure-rate=1.0

# Clean happy path
--simulation.inventory-failure-rate=0.0
--simulation.payment-failure-rate=0.0
--simulation.shipping-failure-rate=0.0
```

### Access Points
| URL | What it is |
|---|---|
| http://localhost:8080/swagger-ui.html | Swagger UI |
| http://localhost:8088 | Temporal UI (workflow visualizer) |
| http://localhost:8080/actuator/health | Health check |
| http://localhost:8080/actuator/prometheus | Prometheus metrics |

---

## Explicit Anti-Patterns (Never Do These)

- Shared database between services
- Calling `OrderRepository` from a controller — always go through application service
- Business logic in `apply()` methods — `apply()` only mutates state, no validation
- Direct state mutation on `Order` — all changes go through command methods which raise events
- `System.currentTimeMillis()` in workflow code — non-deterministic, breaks replay
- Catching `OptimisticLockingException` silently — expose it so callers can retry
- Storing the same event twice — `event_id` has UNIQUE index; catch `DuplicateKeyException` for idempotency
- Publishing to Kafka inside a DB transaction — always use `@TransactionalEventListener(AFTER_COMMIT)`
- Mockito mocks for Temporal activities — use plain POJOs
- Raw `UUID` or `double` in domain code — use `OrderId`/`CustomerId` and `Money(BigDecimal)`

---

## Key Design Decisions (Why)

**Why Event Sourcing instead of CRUD?**
Every state change is a permanent, immutable fact. You can see what happened, when, and why. `PaymentFailedEvent` records the failure reason. `InventoryReleasedEvent` proves compensation ran. CRUD would only show `CANCELLED` with no history.

**Why Temporal instead of queue-based saga?**
A queue-based saga needs: retry tables, DLQs, reconciliation jobs, state machine persistence, distributed coordination. Temporal provides all of that as primitives. Crash recovery, retries, timeouts, and compensation are built-in. The workflow code reads like sequential logic.

**Why both Temporal AND Event Sourcing?**
Temporal manages the *process* (which step, when to retry, how to compensate). Event Sourcing manages the *state* (what happened to the domain). They're complementary — Temporal's activity results are recorded as domain events in the event store.

**Why Hexagonal Architecture?**
The domain knows nothing about Temporal, PostgreSQL, Kafka, or Spring. Swapping Temporal for another orchestrator only touches the infrastructure layer. Domain unit tests run in milliseconds with no infrastructure.

**Why the Outbox Pattern for Kafka?**
Without it: persist order event to DB (succeeds) → publish to Kafka (fails) → notification service never gets the event. With Outbox: DB commit happens first, Kafka publish happens after commit, never inside the same transaction. The `@TransactionalEventListener(AFTER_COMMIT)` is the implementation of this pattern.
