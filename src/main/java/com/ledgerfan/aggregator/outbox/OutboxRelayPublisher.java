package com.ledgerfan.aggregator.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Polls pending rows from {@code outbox_events} and publishes clean semantic domain events
 * onto the Kafka event mesh with idempotency and OpenTelemetry trace-context headers.
 */
@Service
public class OutboxRelayPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final EventMeshTransport eventMeshTransport;
    private final String topicName;
    private final int batchSize;

    public OutboxRelayPublisher(
        OutboxEventRepository outboxEventRepository,
        EventMeshTransport eventMeshTransport,
        @Value("${ledgerfan.outbox.topic:ledger.domain.events.v1}") String topicName,
        @Value("${ledgerfan.outbox.batch-size:100}") int batchSize
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.eventMeshTransport = eventMeshTransport;
        this.topicName = topicName;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${ledgerfan.outbox.poll-interval-ms:500}")
    @Transactional
    public void scheduledRelay() {
        flushPendingEvents();
    }

    @Transactional
    public OutboxRelayFlushResult flushPendingEvents() {
        long startNanos = System.nanoTime();
        List<OutboxEventEntity> pending = outboxEventRepository.findByStatusOrderByCreatedAtAsc(
            OutboxEventStatus.PENDING,
            PageRequest.of(0, batchSize)
        );

        int published = 0;
        long bytesPublished = 0;

        for (OutboxEventEntity event : pending) {
            try {
                Map<String, String> headers = Map.of(
                    "idempotencyKey", event.getIdempotencyKey(),
                    "eventType", event.getEventType(),
                    "schemaVersion", event.getSchemaVersion(),
                    "traceparent", event.getTraceParent() != null ? event.getTraceParent() : ""
                );
                eventMeshTransport.publish(topicName, event.getAggregateId(), event.getPayloadJson(), headers);
                event.markPublished(Instant.now());
                published++;
                bytesPublished += event.getPayloadBytes();
            } catch (Exception ex) {
                log.warn("Failed to relay outbox event id={}: {}", event.getId(), ex.getMessage());
                event.markFailed();
            }
        }

        if (!pending.isEmpty()) {
            outboxEventRepository.saveAll(pending);
        }

        double elapsedMs = Math.round(((System.nanoTime() - startNanos) / 1_000_000.0) * 100.0) / 100.0;
        long remainingPending = outboxEventRepository.countByStatus(OutboxEventStatus.PENDING);

        return new OutboxRelayFlushResult(published, bytesPublished, remainingPending, topicName, elapsedMs);
    }

    public record OutboxRelayFlushResult(
        int publishedCount,
        long bytesPublished,
        long remainingPendingCount,
        String topic,
        double elapsedMs
    ) {
    }
}
