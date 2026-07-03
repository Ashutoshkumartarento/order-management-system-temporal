package com.example.ordermanagement.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.tags.Tag;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * OpenAPI / Swagger Configuration
 *
 * Exposes:
 *   - Swagger UI:  http://localhost:8080/swagger-ui.html
 *   - OpenAPI JSON: http://localhost:8080/v3/api-docs
 *   - OpenAPI YAML: http://localhost:8080/v3/api-docs.yaml
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI orderManagementOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Order Management API")
                        .description("""
                                Production-quality PoC demonstrating **Temporal Workflows** and **Event Sourcing**.
                                
                                ## Key Concepts
                                
                                - **Event Sourcing**: Every state change is stored as an immutable event. \
                                State is rebuilt by replaying events. Use `/history` and `/timeline` endpoints to see this in action.
                                
                                - **Temporal Workflows**: Order fulfillment is orchestrated by a durable Temporal workflow. \
                                Crash-safe, retryable, with built-in saga compensation.
                                
                                - **Signals**: Send `CancelOrder` or `RetryPayment` signals to a running workflow \
                                via the `/signal/*` endpoints.
                                
                                - **Queries**: Read live in-memory workflow state via `GET /workflows/{id}` \
                                (no DB hit — reads directly from the Temporal worker).
                                
                                ## Temporal UI
                                Open **http://localhost:8088** to visualize workflow executions.
                                """)
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Order Management PoC")
                                .url("https://github.com/your-repo/order-management"))
                        .license(new License().name("MIT")))
                .servers(List.of(
                        new Server().url("http://localhost:8080").description("Local development")))
                .tags(List.of(
                        new Tag().name("Orders").description("Order lifecycle management — create, modify, confirm, cancel"),
                        new Tag().name("Workflows").description("Temporal workflow interaction — query status and send signals"),
                        new Tag().name("Observability").description("Health, metrics, and monitoring endpoints")));
    }
}
