package com.example.ordermanagement.infrastructure.persistence;

import com.example.ordermanagement.domain.event.DomainEvent;
import com.example.ordermanagement.domain.exception.OptimisticLockingException;
import com.example.ordermanagement.domain.port.outbound.EventStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

/**
 * Infrastructure Adapter: EventStoreAdapter
 *
 * ═══════════════════════════════════════════════════════════════════
 * RESPONSIBILITY
 * ═══════════════════════════════════════════════════════════════════
 * This is the INFRASTRUCTURE ADAPTER for the EventStore PORT.
 * It provides the PostgreSQL implementation of event storage.
 *
 * Key characteristics:
 *   - APPEND ONLY: no UPDATE or DELETE SQL
 *   - ORDERED: events are always stored and retrieved in version order
 *   - SERIALIZED: events are stored as JSON (JSONB in PostgreSQL)
 *   - IDEMPOTENT: uses unique constraint on (aggregate_id, version) to prevent duplicates
 *
 * ═══════════════════════════════════════════════════════════════════
 * OPTIMISTIC LOCKING IMPLEMENTATION
 * ═══════════════════════════════════════════════════════════════════
 * Before appending events, we check the current version in the DB.
 * If the current version != expectedVersion, another transaction already
 * modified the aggregate. We throw OptimisticLockingException.
 *
 * This check + insert is done in a single DB transaction to be safe.
 * The unique constraint (aggregate_id, version) provides a hard guarantee
 * even if the application-level check is somehow bypassed.
 *
 * ═══════════════════════════════════════════════════════════════════
 * WHY SPRING DATA JDBC (not JPA)?
 * ═══════════════════════════════════════════════════════════════════
 * Event sourcing requires fine-grained control over SQL:
 *   - Explicit append semantics
 *   - Version conflict detection
 *   - No lazy loading, no dirty checking, no proxies
 *
 * Spring Data JDBC gives us a clear, explicit SQL model that matches
 * the append-only event store semantics perfectly.
 *
 * ═══════════════════════════════════════════════════════════════════
 * EVENT SERIALIZATION
 * ═══════════════════════════════════════════════════════════════════
 * Events are serialized to JSON using Jackson.
 * The payload includes the type discriminator (@JsonTypeInfo) so we know
 * which concrete class to deserialize to during replay.
 */
@Repository
public class EventStoreAdapter implements EventStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public EventStoreAdapter(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Appends events to the event store with optimistic locking.
     *
     * OPTIMISTIC LOCKING FLOW:
     * 1. Check current max version for this aggregate
     * 2. Verify it matches expectedVersion
     * 3. Insert all new events atomically
     *
     * The UNIQUE constraint on (aggregate_id, version) provides
     * an additional safety net via DuplicateKeyException.
     */
    @Override
    public void appendEvents(String aggregateId, String aggregateType,
                             List<DomainEvent> events, long expectedVersion) {
        if (events.isEmpty()) return;

        // Optimistic locking check
        long currentVersion = getCurrentVersion(aggregateId);
        if (currentVersion != expectedVersion) {
            throw new OptimisticLockingException(
                    "Aggregate " + aggregateId + " version conflict: expected=" +
                    expectedVersion + " current=" + currentVersion);
        }

// Logging removed

        for (DomainEvent event : events) {
            try {
                String payload = objectMapper.writeValueAsString(event);
                String metadata = buildMetadata(event);

                MapSqlParameterSource params = new MapSqlParameterSource()
                        .addValue("eventId", event.eventId())
                        .addValue("aggregateId", aggregateId)
                        .addValue("aggregateType", aggregateType)
                        .addValue("eventType", event.eventType())
                        .addValue("version", event.version())
                        .addValue("payload", payload)
                        .addValue("metadata", metadata)
                        .addValue("timestamp", java.sql.Timestamp.from(event.occurredAt()));

                jdbcTemplate.update(INSERT_EVENT_SQL, params);

// Logging removed

            } catch (DuplicateKeyException e) {
                // Another transaction inserted the same version — optimistic lock violation
                throw new OptimisticLockingException(
                        "Duplicate event version for aggregate " + aggregateId +
                        " at version " + event.version() + " (concurrent modification detected)");
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize event: " + event.eventType(), e);
            }
        }
    }

    @Override
    public List<DomainEvent> loadEvents(String aggregateId) {
        return loadEvents(aggregateId, 0L);
    }

    /**
     * Loads events for an aggregate starting from a specific version.
     * Used for post-snapshot replay: only load events newer than snapshot.
     */
    @Override
    public List<DomainEvent> loadEvents(String aggregateId, long fromVersion) {
// Logging removed

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("aggregateId", aggregateId)
                .addValue("fromVersion", fromVersion);

        List<DomainEvent> events = jdbcTemplate.query(
                LOAD_EVENTS_SQL, params, this::mapRowToEvent);

// Logging removed
        return events;
    }

    @Override
    public long getCurrentVersion(String aggregateId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("aggregateId", aggregateId);

        Long version = jdbcTemplate.queryForObject(GET_VERSION_SQL, params, Long.class);
        return version != null ? version : 0L;
    }

    // ─────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────

    private DomainEvent mapRowToEvent(ResultSet rs, int rowNum) throws SQLException {
        try {
            String payload = rs.getString("payload");
            return objectMapper.readValue(payload, DomainEvent.class);
        } catch (Exception e) {
            throw new SQLException("Failed to deserialize event at row " + rowNum, e);
        }
    }

    private String buildMetadata(DomainEvent event) {
        try {
            // Include correlation information for tracing
            Map<String, String> metadata = new HashMap<>();
            metadata.put("eventId", event.eventId().toString());
            metadata.put("aggregateId", event.aggregateId());
            // In production: add correlationId, causationId from MDC
            return objectMapper.writeValueAsString(metadata);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    // ─────────────────────────────────────────────────────
    // SQL Constants
    // ─────────────────────────────────────────────────────

    private static final String INSERT_EVENT_SQL = """
            INSERT INTO event_store (
                event_id, aggregate_id, aggregate_type, event_type,
                version, payload, metadata, timestamp
            ) VALUES (
                :eventId, :aggregateId, :aggregateType, :eventType,
                :version, :payload::jsonb, :metadata::jsonb, :timestamp
            )
            """;

    private static final String LOAD_EVENTS_SQL = """
            SELECT event_id, aggregate_id, aggregate_type, event_type,
                   version, payload, metadata, timestamp
            FROM event_store
            WHERE aggregate_id = :aggregateId
              AND version > :fromVersion
            ORDER BY version ASC
            """;

    private static final String GET_VERSION_SQL = """
            SELECT COALESCE(MAX(version), 0)
            FROM event_store
            WHERE aggregate_id = :aggregateId
            """;
}
