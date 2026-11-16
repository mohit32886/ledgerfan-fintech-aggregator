package com.ledgerfan.aggregator.outbox;

/**
 * Lifecycle state of an event persisted in the transactional outbox table.
 */
public enum OutboxEventStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
