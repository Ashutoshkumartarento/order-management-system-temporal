package com.example.ordermanagement.infrastructure.temporal.workflow;

import com.example.ordermanagement.infrastructure.temporal.workflow.model.WorkflowDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

@Component
public class YamlWorkflowLoader {

    private final ObjectMapper mapper;

    public YamlWorkflowLoader() {
        this.mapper = new ObjectMapper(new YAMLFactory());
    }

    public WorkflowDefinition loadWorkflow(String resourcePath) {
        try (InputStream inputStream = new ClassPathResource(resourcePath).getInputStream()) {
            return mapper.readValue(inputStream, WorkflowDefinition.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load workflow definition from YAML: " + resourcePath, e);
        }
    }
}
