package com.ledgerfan.aggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the LedgerFan High-Throughput Fintech Account Aggregator & Outbox Event Mesh.
 *
 * <p>Uses Java 21 Virtual Threads and Structured Concurrency ({@code StructuredTaskScope}) for
 * sub-100ms fan-out aggregation across 12 banking upstreams, paired with a zero-dual-write
 * Transactional Outbox + Redis Resource Cache event mesh.
 */
@SpringBootApplication
@EnableScheduling
public class LedgerFanAggregatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerFanAggregatorApplication.class, args);
    }
}
