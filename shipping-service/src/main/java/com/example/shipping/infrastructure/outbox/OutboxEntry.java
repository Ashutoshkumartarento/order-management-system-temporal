package com.example.shipping.infrastructure.outbox;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Table("outbox_events")
public class OutboxEntry implements Persistable<Long> {

    @Id
    private final Long id;
    private final UUID eventId;
    private final String aggregateId;
    private final String eventType;
    private final String topic;
    private final String payload;
    private final Instant createdAt;
    private final Instant publishedAt;

    @Transient
    private final boolean isNew;

    @PersistenceCreator
    public OutboxEntry(Long id, UUID eventId, String aggregateId, String eventType,
                       String topic, String payload, Instant createdAt, Instant publishedAt) {
        this(id, eventId, aggregateId, eventType, topic, payload, createdAt, publishedAt, false);
    }

    public OutboxEntry(Long id, UUID eventId, String aggregateId, String eventType,
                       String topic, String payload, Instant createdAt, Instant publishedAt,
                       boolean isNew) {
        this.id = id;
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.topic = topic;
        this.payload = payload;
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
        this.isNew = isNew;
    }

    public static OutboxEntry create(UUID eventId, String aggregateId, String eventType,
                                     String topic, String payload) {
        return new OutboxEntry(null, eventId, aggregateId, eventType, topic, payload,
                Instant.now(), null, true);
    }

    @Override public Long getId()    { return id; }
    @Override public boolean isNew() { return isNew; }

    public UUID    eventId()      { return eventId; }
    public String  aggregateId()  { return aggregateId; }
    public String  eventType()    { return eventType; }
    public String  topic()        { return topic; }
    public String  payload()      { return payload; }
    public Instant createdAt()    { return createdAt; }
    public Instant publishedAt()  { return publishedAt; }
}
