package com.example.payment.infrastructure.outbox;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface OutboxRepository extends CrudRepository<OutboxEntry, Long> {

    @Query("SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY created_at LIMIT 100")
    List<OutboxEntry> findUnpublished();

    @Modifying
    @Query("UPDATE outbox_events SET published_at = :publishedAt WHERE id = :id AND published_at IS NULL")
    int markPublished(Long id, Instant publishedAt);
}
