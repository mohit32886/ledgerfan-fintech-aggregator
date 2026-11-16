package com.ledgerfan.aggregator.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;

/**
 * Publishes domain events to Kafka (when enabled in docker/production mode) and maintains
 * a bounded in-memory audit ring buffer for sub-millisecond verification and local benchmarks.
 */
@Component
public class HybridEventMeshTransport implements EventMeshTransport {

    private static final Logger log = LoggerFactory.getLogger(HybridEventMeshTransport.class);
    private static final int MAX_RETAINED_MESSAGES = 500;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final boolean kafkaEnabled;
    private final ConcurrentLinkedDeque<PublishedTopicMessage> recentQueue = new ConcurrentLinkedDeque<>();
    private final LongAdder publishedCount = new LongAdder();
    private final LongAdder publishedBytes = new LongAdder();

    public HybridEventMeshTransport(
        ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider,
        @Value("${ledgerfan.outbox.kafka-enabled:false}") boolean kafkaEnabled
    ) {
        this.kafkaTemplate = kafkaTemplateProvider.getIfAvailable();
        this.kafkaEnabled = kafkaEnabled && this.kafkaTemplate != null;
    }

    @Override
    public void publish(String topic, String partitionKey, String payloadJson, Map<String, String> headers) {
        byte[] payloadBytes = payloadJson.getBytes(StandardCharsets.UTF_8);

        if (kafkaEnabled) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, payloadJson);
            headers.forEach((k, v) -> {
                if (v != null) {
                    record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
                }
            });
            try {
                kafkaTemplate.send(record).get();
            } catch (Exception ex) {
                log.error("Failed to publish outbox event to Kafka topic={} key={}: {}", topic, partitionKey, ex.getMessage());
                throw new IllegalStateException("Kafka publish failure", ex);
            }
        }

        PublishedTopicMessage entry = new PublishedTopicMessage(
            topic,
            partitionKey,
            payloadBytes.length,
            payloadJson,
            headers,
            Instant.now()
        );
        recentQueue.addFirst(entry);
        while (recentQueue.size() > MAX_RETAINED_MESSAGES) {
            recentQueue.pollLast();
        }
        publishedCount.increment();
        publishedBytes.add(payloadBytes.length);
    }

    @Override
    public List<PublishedTopicMessage> recentMessages(int limit) {
        List<PublishedTopicMessage> snapshot = new ArrayList<>();
        int count = 0;
        for (PublishedTopicMessage msg : recentQueue) {
            if (count++ >= limit) {
                break;
            }
            snapshot.add(msg);
        }
        return Collections.unmodifiableList(snapshot);
    }

    @Override
    public long totalPublishedCount() {
        return publishedCount.sum();
    }

    @Override
    public long totalPublishedBytes() {
        return publishedBytes.sum();
    }
}
