package com.ledgerfan.aggregator.observability;

import java.util.Map;

/**
 * Represents a single OpenTelemetry span inside an account aggregation waterfall trace.
 */
public record SpanWaterfallEntry(
    String spanId,
    String parentSpanId,
    String operationName,
    String upstreamName,
    String protocol,
    String status,
    long startOffsetMicros,
    long durationMicros,
    double durationMs,
    String virtualThreadName,
    boolean cacheHit,
    Map<String, String> attributes
) {
}
