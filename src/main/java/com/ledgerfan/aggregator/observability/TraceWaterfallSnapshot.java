package com.ledgerfan.aggregator.observability;

import java.time.Instant;
import java.util.List;

/**
 * Captured OpenTelemetry distributed trace waterfall for a 12-upstream fan-out aggregation call.
 */
public record TraceWaterfallSnapshot(
    String traceId,
    String rootSpanId,
    String accountId,
    Instant capturedAt,
    double totalDurationMs,
    long slaTargetMs,
    boolean slaMet,
    double parallelismFactor,
    List<SpanWaterfallEntry> spans,
    String asciiWaterfall
) {
}
