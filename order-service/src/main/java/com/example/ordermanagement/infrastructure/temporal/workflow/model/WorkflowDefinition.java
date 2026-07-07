package com.example.ordermanagement.infrastructure.temporal.workflow.model;

import java.util.List;

public record WorkflowDefinition(
    String name,
    List<WorkflowStep> steps
) {}
