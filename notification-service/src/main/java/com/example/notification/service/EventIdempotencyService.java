package com.example.notification.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

/**
 * Redis-backed idempotency check for Kafka consumer deduplication.
 *
 * Key schema:  notification:processed:{eventId}
 * TTL:         24 hours — sufficient to cover Kafka redelivery windows after restarts.
 *
 * Uses SET NX (set-if-not-exists): atomic, no race condition between check and set.
 * Returns true on the FIRST occurrence of an eventId; false on duplicates.
 */
@Service
public class EventIdempotencyService {

    private static final Duration TTL = Duration.ofHours(24);
    private static final String KEY_PREFIX = "notification:processed:";

    private final StringRedisTemplate redisTemplate;

    public EventIdempotencyService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Returns true if this is the first time the eventId has been seen.
     * Atomically marks the eventId as processed with a 24-hour TTL.
     */
    public boolean isFirstOccurrence(UUID eventId) {
        Boolean inserted = redisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + eventId, "1", TTL);
        return Boolean.TRUE.equals(inserted);
    }
}