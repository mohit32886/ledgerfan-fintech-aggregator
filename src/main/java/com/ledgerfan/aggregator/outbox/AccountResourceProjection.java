package com.ledgerfan.aggregator.outbox;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Hot Redis resource cache projection hydrated on write alongside the Transactional Outbox,
 * enabling thin Kafka domain events without downstream monolith callbacks.
 */
public record AccountResourceProjection(
    String accountId,
    BigDecimal latestBalance,
    String currency,
    UUID lastTransactionId,
    long totalPostedCount,
    Instant lastUpdatedAt
) {
}
