package com.ledgerfan.aggregator.outbox;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Result returned after atomically committing a ledger transaction, its transactional outbox row,
 * and updating the Redis resource projection cache.
 */
public record LedgerPostingResult(
    UUID transactionId,
    UUID outboxEventId,
    String accountId,
    BigDecimal balanceAfter,
    int outboxPayloadBytes,
    String traceParent,
    OutboxEventStatus outboxStatus,
    Instant committedAt
) {
}
