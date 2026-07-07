package com.example.ordermanagement.infrastructure.temporal;

import com.example.ordermanagement.domain.port.outbound.WorkflowPort;
import com.example.ordermanagement.domain.valueobject.OrderId;
import com.example.ordermanagement.infrastructure.temporal.workflow.OrderFulfillmentWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.example.ordermanagement.infrastructure.temporal.workflow.YamlWorkflowLoader;

import java.time.Duration;

/**
 * Infrastructure Adapter: WorkflowPortAdapter
 *
 * ═══════════════════════════════════════════════════════════════════
 * RESPONSIBILITY
 * ═══════════════════════════════════════════════════════════════════
 * Implements the WorkflowPort interface using Temporal's Java SDK.
 * This is where Spring Boot meets Temporal.
 *
 * The application layer calls WorkflowPort (the interface).
 * This adapter translates those calls into Temporal SDK operations.
 *
 * ═══════════════════════════════════════════════════════════════════
 * TEMPORAL CONCEPTS USED HERE
 * ═══════════════════════════════════════════════════════════════════
 *
 * WorkflowClient: The entry point to Temporal from application code.
 *   - Creates workflow stubs for sending signals and queries
 *   - Starts new workflow executions
 *
 * WorkflowOptions: Configuration for workflow execution
 *   - workflowId: deterministic ID derived from business ID
 *   - taskQueue: which worker pool handles this workflow
 *   - executionTimeout: maximum time the workflow can run
 *   - runTimeout: maximum time for a single run (after which Temporal continues-as-new)
 *
 * WorkflowExecutionAlreadyStarted: Temporal's idempotency mechanism.
 *   If we try to start a workflow with the same workflowId, Temporal
 *   returns this exception instead of creating a duplicate.
 *   We handle this gracefully — it means we've already started it.
 *
 * ═══════════════════════════════════════════════════════════════════
 * TEMPORAL QUERY EXPLAINED
 * ═══════════════════════════════════════════════════════════════════
 * Queries are SYNCHRONOUS reads of workflow in-memory state.
 * The Temporal server routes the query to the worker executing the workflow.
 * The worker calls the @QueryMethod on the workflow instance and returns the result.
 * No DB queries, no replay — just reading current workflow local variables.
 * This makes queries extremely fast (sub-millisecond typically).
 */
@Component
public class WorkflowPortAdapter implements WorkflowPort {

    private static final Logger log = LoggerFactory.getLogger(WorkflowPortAdapter.class);

    public static final String TASK_QUEUE = "ORDER_FULFILLMENT_QUEUE";

    private final WorkflowClient workflowClient;

    @Value("${simulation.step-delay-ms:0}")
    private long stepDelayMs;

    public WorkflowPortAdapter(WorkflowClient workflowClient) {
        this.workflowClient = workflowClient;
    }

    @Override
    public String startFulfillmentWorkflow(OrderId orderId, String workflowId) {
        log.info("Starting fulfillment workflow {} for order {}", workflowId, orderId);

        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                // Maximum time the entire workflow can run (including waiting for signals)
                .setWorkflowExecutionTimeout(Duration.ofHours(24))
                // Maximum time for a single workflow run
                .setWorkflowRunTimeout(Duration.ofHours(2))
                .build();

        OrderFulfillmentWorkflow workflow = workflowClient.newWorkflowStub(
                OrderFulfillmentWorkflow.class, options);

        try {
            // Start asynchronously — don't wait for workflow to complete
            YamlWorkflowLoader loader = new YamlWorkflowLoader();
            com.example.ordermanagement.infrastructure.temporal.workflow.model.WorkflowDefinition definition = loader.loadWorkflow("workflow-config.yml");
            WorkflowClient.start(workflow::fulfill, orderId.toString(), stepDelayMs, definition);
            log.info("Workflow {} started on task queue {}", workflowId, TASK_QUEUE);
        } catch (WorkflowExecutionAlreadyStarted e) {
            // Idempotency: workflow already running with this ID — that's fine
            log.info("Workflow {} already running — skipping start (idempotent)", workflowId);
        }

        return workflowId;
    }

    @Override
    public void sendCancelSignal(String workflowId, String reason) {
        log.info("Sending CancelOrder signal to workflow {}: {}", workflowId, reason);

        OrderFulfillmentWorkflow workflow = workflowClient.newWorkflowStub(
                OrderFulfillmentWorkflow.class, workflowId);

        try {
            workflow.cancelOrder(reason);
            log.info("CancelOrder signal delivered to workflow {}", workflowId);
        } catch (Exception e) {
            // Workflow may have already completed — log but don't fail
            log.warn("Could not deliver CancelOrder signal to workflow {} (may have already completed): {}", workflowId, e.getMessage());
        }
    }

    @Override
    public void sendRetryPaymentSignal(String workflowId) {
        log.info("Sending RetryPayment signal to workflow {}", workflowId);

        OrderFulfillmentWorkflow workflow = workflowClient.newWorkflowStub(
                OrderFulfillmentWorkflow.class, workflowId);

        try {
            workflow.retryPayment();
        } catch (Exception e) {
            log.warn("Could not deliver RetryPayment signal to workflow {} (may have already completed): {}", workflowId, e.getMessage());
        }
    }

    @Override
    public WorkflowStatusResult queryWorkflowStatus(String workflowId) {
        try {
            OrderFulfillmentWorkflow workflow = workflowClient.newWorkflowStub(
                    OrderFulfillmentWorkflow.class, workflowId);

            OrderFulfillmentWorkflow.WorkflowProgress progress = workflow.getProgress();
            return new WorkflowStatusResult(
                    workflowId,
                    progress.status(),
                    progress.currentStep(),
                    progress.retryCount()
            );
        } catch (Exception e) {
            log.debug("Workflow {} not found or completed — returning UNKNOWN status: {}", workflowId, e.getMessage());
            return new WorkflowStatusResult(workflowId, "UNKNOWN", "UNKNOWN", 0);
        }
    }
}
