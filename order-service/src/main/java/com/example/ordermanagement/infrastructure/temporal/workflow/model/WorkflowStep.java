package com.example.ordermanagement.infrastructure.temporal.workflow.model;

public record WorkflowStep(
    String name,
    String activityName,
    String methodName,
    String compensationActivity,
    String compensationMethod,
    String compensationStep,
    Integer timeoutSeconds,
    Integer maxAttempts
) {
    public WorkflowStep {
        if (timeoutSeconds == null) timeoutSeconds = 30;
        if (maxAttempts == null) maxAttempts = 3;
    }
}
