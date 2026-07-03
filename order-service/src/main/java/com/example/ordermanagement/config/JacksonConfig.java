package com.example.ordermanagement.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configuration: JacksonConfig
 *
 * Configures Jackson ObjectMapper for event serialization/deserialization.
 *
 * KEY SETTINGS:
 * - JavaTimeModule: Handles Java 8+ date/time types (Instant, LocalDate)
 *   Events contain Instant timestamps — requires this module.
 *
 * - WRITE_DATES_AS_TIMESTAMPS: false → ISO-8601 string format in JSON
 *   Human-readable in event store, easier debugging.
 *
 * - FAIL_ON_UNKNOWN_PROPERTIES: false → Schema evolution compatibility
 *   Old events may not have new fields. We should load them gracefully.
 *
 * - ParameterNamesModule: Allows Jackson to use constructor parameter names
 *   for deserialization without @JsonProperty annotations on every field.
 */
@Configuration
public class JacksonConfig {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Java time support (Instant in events)
        mapper.registerModule(new JavaTimeModule());
        // Use constructor parameter names for deserialization
        mapper.registerModule(new ParameterNamesModule());

        // Serialize Instant as "2024-01-15T14:30:00Z", not as epoch millis
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Critical for event evolution: new code reading old events with missing fields
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // Indent output for readability in event store (optional — remove in prod for space)
        // mapper.enable(SerializationFeature.INDENT_OUTPUT);

        return mapper;
    }
}
