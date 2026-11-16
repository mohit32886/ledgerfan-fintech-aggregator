package com.ledgerfan.aggregator.outbox;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Compact semantic domain event emitted to Kafka via the Transactional Outbox relay
 * (replaces bloated multi-table CDC before/after row envelopes).
 */
public record LedgerTransactionPostedEvent(
    UUID eventId,
    String accountId,
    String counterpartyAccountId,
    UUID transactionId,
    String transactionType,
    BigDecimal amount,
    String currency,
    BigDecimal balanceAfter,
    Instant occurredAt
) {
}
