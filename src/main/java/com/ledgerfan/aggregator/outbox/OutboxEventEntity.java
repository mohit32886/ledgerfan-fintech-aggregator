package com.ledgerfan.aggregator.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Transactional Outbox row written in the same ACID commit as {@link LedgerTransactionEntity}.
 * Eliminates dual-write anomalies and noisy table-level CDC amplification.
 */
@Entity
@Table(
    name = "outbox_events",
    indexes = {
        @Index(name = "idx_outbox_status_created", columnList = "status, createdAt")
    }
)
public class OutboxEventEntity {

    @Id
    private UUID id;

    @Column(nullable = false, length = 64)
    private String aggregateType;

    @Column(nullable = false, length = 64)
    private String aggregateId;

    @Column(nullable = false, length = 96)
    private String eventType;

    @Column(nullable = false, length = 16)
    private String schemaVersion;

    @Column(nullable = false, unique = true, length = 96)
    private String idempotencyKey;

    @Column(length = 128)
    private String traceParent;

    @Column(nullable = false, length = 4096)
    private String payloadJson;

    @Column(nullable = false)
    private int payloadBytes;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private OutboxEventStatus status;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant publishedAt;

    protected OutboxEventEntity() {
    }

    public OutboxEventEntity(
        UUID id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String schemaVersion,
        String idempotencyKey,
        String traceParent,
        String payloadJson,
        int payloadBytes,
        OutboxEventStatus status,
        Instant createdAt
    ) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.schemaVersion = schemaVersion;
        this.idempotencyKey = idempotencyKey;
        this.traceParent = traceParent;
        this.payloadJson = payloadJson;
        this.payloadBytes = payloadBytes;
        this.status = status;
        this.createdAt = createdAt;
    }

    public void markPublished(Instant publishedAt) {
        this.status = OutboxEventStatus.PUBLISHED;
        this.publishedAt = publishedAt;
    }

    public void markFailed() {
        this.status = OutboxEventStatus.FAILED;
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getTraceParent() {
        return traceParent;
    }

    public String getPayloadJson() {
        return payloadJson;
    }

    public int getPayloadBytes() {
        return payloadBytes;
    }

    public OutboxEventStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
