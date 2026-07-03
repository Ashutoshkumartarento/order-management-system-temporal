package com.example.ordermanagement.api.controller;

import com.example.ordermanagement.domain.port.inbound.OrderQueryUseCase;
import com.example.ordermanagement.domain.port.outbound.WorkflowPort;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST Controller: WorkflowController
 *
 * Direct interaction with Temporal workflows — query state and send signals.
 * workflowId format: "order-fulfillment-{orderId}"
 */
@RestController
@RequestMapping("/workflows")
@Tag(name = "Workflows", description = """
        Direct Temporal workflow interaction. The `workflowId` follows the pattern
        `order-fulfillment-{orderId}` and is returned by `POST /orders/{id}/confirm`.
        
        **Query** reads live in-memory workflow state (no DB, no event replay).
        **Signals** deliver asynchronous messages to a running workflow.
        
        Open the **Temporal UI at http://localhost:8088** to visualize all workflow executions.
        """)
public class WorkflowController {

    private final OrderQueryUseCase queryService;
    private final WorkflowPort workflowPort;

    public WorkflowController(OrderQueryUseCase queryService, WorkflowPort workflowPort) {
        this.queryService = queryService;
        this.workflowPort = workflowPort;
    }

    @Operation(
        summary = "Query workflow status",
        description = """
                Reads the **live in-memory state** of a Temporal workflow via a Temporal Query.
                
                This is fundamentally different from reading the event store:
                - **Event store query**: reads persisted domain events from PostgreSQL
                - **Temporal query**: reads current variables inside the running workflow process (no DB hit)
                
                Returns: `status`, `currentStep`, `retryCount`, and whether cancellation was requested.
                
                Open **http://localhost:8088** for the full graphical workflow execution view.
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Workflow status",
            content = @Content(schema = @Schema(implementation = WorkflowPort.WorkflowStatusResult.class))),
        @ApiResponse(responseCode = "200", description = "Returns UNKNOWN status if workflow is not found or already completed")
    })
    @GetMapping("/{workflowId}")
    public ResponseEntity<WorkflowPort.WorkflowStatusResult> getWorkflowStatus(
            @Parameter(description = "Temporal workflow ID — format: order-fulfillment-{orderId}", required = true)
            @PathVariable String workflowId) {

        return ResponseEntity.ok(queryService.getWorkflowStatus(workflowId));
    }

    @Operation(
        summary = "Send CancelOrder signal",
        description = """
                Sends a **CancelOrder signal** to the running Temporal workflow.
                
                Temporal delivers signals asynchronously — even if the workflow is
                currently mid-activity, the signal is queued and processed at the
                next safe checkpoint.
                
                The workflow then executes saga compensation:
                - If inventory was reserved → `releaseInventory` (compensating transaction)
                - If payment was taken → `refundPayment` (compensating transaction)
                - Finally records `OrderCancelledEvent` in the event store
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "Cancel signal sent — compensation will run asynchronously"),
        @ApiResponse(responseCode = "200", description = "Signal silently ignored if workflow already completed")
    })
    @PostMapping("/{workflowId}/signal/cancel")
    public ResponseEntity<Map<String, String>> sendCancelSignal(
            @Parameter(description = "Temporal workflow ID", required = true)
            @PathVariable String workflowId,
            @RequestBody Map<String, String> body) {

        String reason = body.getOrDefault("reason", "Manual cancellation via API");
// Logging removed
        workflowPort.sendCancelSignal(workflowId, reason);
        return ResponseEntity.accepted().body(Map.of("message", "Cancel signal sent", "workflowId", workflowId));
    }

    @Operation(
        summary = "Send RetryPayment signal",
        description = """
                Sends a **RetryPayment signal** to a workflow waiting for customer action
                after a transient payment failure.
                
                The workflow uses `Workflow.await(Duration.ofMinutes(5), () -> retryRequested)`
                to pause execution. This signal sets `retryRequested = true`, waking the workflow
                so it re-attempts the `PaymentActivity`.
                
                Has no effect if the workflow is not in `WAITING_FOR_PAYMENT_RETRY` state.
                """)
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "RetryPayment signal sent")
    })
    @PostMapping("/{workflowId}/signal/retry-payment")
    public ResponseEntity<Map<String, String>> sendRetryPaymentSignal(
            @Parameter(description = "Temporal workflow ID", required = true)
            @PathVariable String workflowId) {

// Logging removed
        workflowPort.sendRetryPaymentSignal(workflowId);
        return ResponseEntity.accepted().body(Map.of("message", "RetryPayment signal sent", "workflowId", workflowId));
    }
}
