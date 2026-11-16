package com.ledgerfan.aggregator.outbox;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Transport abstraction for publishing outbox domain events to Kafka and an inspectable
 * in-memory topic log for deterministic testing and observability.
 */
public interface EventMeshTransport {

    void publish(String topic, String partitionKey, String payloadJson, Map<String, String> headers);

    List<PublishedTopicMessage> recentMessages(int limit);

    long totalPublishedCount();

    long totalPublishedBytes();

    record PublishedTopicMessage(
        String topic,
        String partitionKey,
        int payloadBytes,
        String payloadJson,
        Map<String, String> headers,
        Instant publishedAt
    ) {
    }
}
