package com.ledgerfan.aggregator.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Authoritative ledger transaction persisted atomically alongside its outbox domain event.
 */
@Entity
@Table(
    name = "ledger_transactions",
    indexes = {
        @Index(name = "idx_ledger_tx_account_created", columnList = "accountId, createdAt")
    }
)
public class LedgerTransactionEntity {

    @Id
    private UUID id;

    @Column(nullable = false, length = 64)
    private String accountId;

    @Column(nullable = false, length = 64)
    private String counterpartyAccountId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 8)
    private String currency;

    @Column(nullable = false, length = 32)
    private String transactionType;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal balanceAfter;

    @Column(length = 128)
    private String reference;

    @Column(nullable = false)
    private Instant createdAt;

    protected LedgerTransactionEntity() {
    }

    public LedgerTransactionEntity(
        UUID id,
        String accountId,
        String counterpartyAccountId,
        BigDecimal amount,
        String currency,
        String transactionType,
        BigDecimal balanceAfter,
        String reference,
        Instant createdAt
    ) {
        this.id = id;
        this.accountId = accountId;
        this.counterpartyAccountId = counterpartyAccountId;
        this.amount = amount;
        this.currency = currency;
        this.transactionType = transactionType;
        this.balanceAfter = balanceAfter;
        this.reference = reference;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getCounterpartyAccountId() {
        return counterpartyAccountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public BigDecimal getBalanceAfter() {
        return balanceAfter;
    }

    public String getReference() {
        return reference;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
