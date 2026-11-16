package com.ledgerfan.aggregator.orchestrator;

import java.util.List;
import java.util.Map;

/**
 * Execution telemetry and degradation summary attached to every aggregated financial profile response.
 */
public record AggregationMetadata(
    String accountId,
    String traceId,
    String status,
    double totalElapsedMs,
    double sumSequentialUpstreamMs,
    double speedupFactor,
    long slaTargetMs,
    boolean slaMet,
    int totalUpstreams,
    int succeededCount,
    int cacheHitCount,
    int staleFallbackCount,
    int degradedCount,
    List<String> degradedUpstreams,
    Map<String, String> upstreamStatuses,
    Map<String, Double> upstreamTimingsMs
) {
}
