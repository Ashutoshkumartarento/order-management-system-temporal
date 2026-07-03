package com.example.ordermanagement.infrastructure.persistence;

import com.example.ordermanagement.domain.aggregate.OrderSnapshot;
import com.example.ordermanagement.domain.port.outbound.SnapshotStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * Infrastructure Adapter: SnapshotStoreAdapter
 *
 * Persists and loads aggregate snapshots.
 * Snapshots are stored in the order_snapshots table as JSON.
 *
 * WHEN TO SNAPSHOT:
 * After every SNAPSHOT_THRESHOLD (50) events are stored for an aggregate.
 * The OrderRepositoryAdapter checks this and calls saveSnapshot() automatically.
 *
 * SNAPSHOT EVOLUTION:
 * As the domain model evolves, old snapshot JSON may be missing new fields.
 * Jackson's DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES = false (default)
 * handles forward compatibility (new code, old snapshot).
 * Use @JsonProperty(defaultValue = "...") for backward compatibility.
 */
@Repository
public class SnapshotStoreAdapter implements SnapshotStore {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public SnapshotStoreAdapter(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void saveSnapshot(OrderSnapshot snapshot) {
        try {
            String snapshotData = objectMapper.writeValueAsString(snapshot);

            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("snapshotId", snapshot.snapshotId())
                    .addValue("aggregateId", snapshot.aggregateId())
                    .addValue("version", snapshot.version())
                    .addValue("snapshotData", snapshotData)
                    .addValue("createdAt", java.sql.Timestamp.from(snapshot.takenAt()));

            // Upsert — we only need the latest snapshot
            jdbcTemplate.update(UPSERT_SNAPSHOT_SQL, params);

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize snapshot for aggregate " +
                    snapshot.aggregateId(), e);
        }
    }

    @Override
    public Optional<OrderSnapshot> loadLatestSnapshot(String aggregateId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("aggregateId", aggregateId);

        List<OrderSnapshot> snapshots = jdbcTemplate.query(
                LOAD_LATEST_SNAPSHOT_SQL, params, this::mapRowToSnapshot);

        Optional<OrderSnapshot> result = snapshots.isEmpty()
                ? Optional.empty()
                : Optional.of(snapshots.getFirst());

        return result;
    }

    private OrderSnapshot mapRowToSnapshot(ResultSet rs, int rowNum) throws SQLException {
        try {
            return objectMapper.readValue(rs.getString("snapshot_data"), OrderSnapshot.class);
        } catch (Exception e) {
            throw new SQLException("Failed to deserialize snapshot", e);
        }
    }

    private static final String UPSERT_SNAPSHOT_SQL = """
            INSERT INTO order_snapshots (snapshot_id, aggregate_id, version, snapshot_data, created_at)
            VALUES (:snapshotId, :aggregateId, :version, :snapshotData::jsonb, :createdAt)
            ON CONFLICT (aggregate_id)
            DO UPDATE SET
                snapshot_id   = EXCLUDED.snapshot_id,
                version       = EXCLUDED.version,
                snapshot_data = EXCLUDED.snapshot_data,
                created_at    = EXCLUDED.created_at
            """;

    private static final String LOAD_LATEST_SNAPSHOT_SQL = """
            SELECT snapshot_id, aggregate_id, version, snapshot_data, created_at
            FROM order_snapshots
            WHERE aggregate_id = :aggregateId
            """;
}
