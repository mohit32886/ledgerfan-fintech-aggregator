package com.ledgerfan.aggregator.domain;

/**
 * Immutable result envelope produced by each virtual-thread upstream call.
 */
public record UpstreamCallOutcome<T>(
    String upstreamName,
    UpstreamProtocol protocol,
    UpstreamCriticality criticality,
    UpstreamStatus status,
    T payload,
    long latencyMicros,
    double latencyMs,
    boolean cacheHit,
    boolean degraded,
    String errorMessage,
    String virtualThreadName,
    long startOffsetMicros
) {
}
