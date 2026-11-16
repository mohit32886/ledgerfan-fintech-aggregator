package com.ledgerfan.aggregator.observability;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Bounded in-memory collector for OpenTelemetry fan-out trace waterfalls.
 * Enables immediate inspection of virtual-thread concurrency timelines via API and tests,
 * alongside OTLP export to Tempo/Jaeger.
 */
@Component
public class InMemoryWaterfallExporter {

    private static final int BAR_WIDTH = 44;

    private final int maxRetainedTraces;
    private final ConcurrentLinkedDeque<String> traceOrder = new ConcurrentLinkedDeque<>();
    private final Map<String, TraceWaterfallSnapshot> byTraceId = new ConcurrentHashMap<>();
    private final Map<String, TraceWaterfallSnapshot> latestByAccountId = new ConcurrentHashMap<>();

    public InMemoryWaterfallExporter(
        @Value("${ledgerfan.observability.max-retained-traces:200}") int maxRetainedTraces
    ) {
        this.maxRetainedTraces = Math.max(10, maxRetainedTraces);
    }

    public TraceWaterfallSnapshot record(
        String traceId,
        String rootSpanId,
        String accountId,
        long totalDurationMicros,
        long slaTargetMs,
        List<SpanWaterfallEntry> rawSpans
    ) {
        List<SpanWaterfallEntry> orderedSpans = rawSpans.stream()
            .sorted(Comparator.comparingLong(SpanWaterfallEntry::startOffsetMicros)
                .thenComparing(SpanWaterfallEntry::upstreamName))
            .toList();

        double totalMs = round2(totalDurationMicros / 1000.0);
        double upstreamSumMicros = orderedSpans.stream()
            .filter(s -> !"INTERNAL".equals(s.protocol()))
            .mapToLong(SpanWaterfallEntry::durationMicros)
            .sum();
        double parallelismFactor = totalDurationMicros > 0
            ? round2(upstreamSumMicros / totalDurationMicros)
            : 1.0;

        String ascii = renderAsciiWaterfall(traceId, accountId, totalDurationMicros, slaTargetMs, parallelismFactor, orderedSpans);

        TraceWaterfallSnapshot snapshot = new TraceWaterfallSnapshot(
            traceId,
            rootSpanId,
            accountId,
            Instant.now(),
            totalMs,
            slaTargetMs,
            totalMs <= slaTargetMs,
            parallelismFactor,
            orderedSpans,
            ascii
        );

        byTraceId.put(traceId, snapshot);
        latestByAccountId.put(accountId, snapshot);
        traceOrder.addLast(traceId);

        while (traceOrder.size() > maxRetainedTraces) {
            String evicted = traceOrder.pollFirst();
            if (evicted != null) {
                byTraceId.remove(evicted);
            }
        }

        return snapshot;
    }

    public Optional<TraceWaterfallSnapshot> findByTraceId(String traceId) {
        return Optional.ofNullable(byTraceId.get(traceId));
    }

    public Optional<TraceWaterfallSnapshot> findLatestByAccountId(String accountId) {
        return Optional.ofNullable(latestByAccountId.get(accountId));
    }

    public List<TraceWaterfallSnapshot> recentTraces(int limit) {
        return traceOrder.stream()
            .map(byTraceId::get)
            .filter(t -> t != null)
            .sorted(Comparator.comparing(TraceWaterfallSnapshot::capturedAt).reversed())
            .limit(Math.max(1, limit))
            .toList();
    }

    private String renderAsciiWaterfall(
        String traceId,
        String accountId,
        long totalDurationMicros,
        long slaTargetMs,
        double parallelismFactor,
        List<SpanWaterfallEntry> spans
    ) {
        long timelineMicros = Math.max(1L, totalDurationMicros);
        double totalMs = round2(timelineMicros / 1000.0);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
            "Trace Waterfall [traceId=%s, account=%s, wall=%.2fms, sla=%dms, parallelism=%.2fx]%n",
            traceId, accountId, totalMs, slaTargetMs, parallelismFactor
        ));
        sb.append(String.format(
            "%-30s %-8s %-14s %-9s %s%n",
            "SPAN / UPSTREAM", "PROTO", "STATUS", "DURATION", "TIMELINE (0 -> " + String.format("%.1fms", totalMs) + ")"
        ));
        sb.append("-".repeat(114)).append('\n');

        for (SpanWaterfallEntry span : spans) {
            int startCol = (int) Math.min(BAR_WIDTH - 1, Math.floor((span.startOffsetMicros() * 1.0 / timelineMicros) * BAR_WIDTH));
            int lenCols = (int) Math.max(1, Math.ceil((span.durationMicros() * 1.0 / timelineMicros) * BAR_WIDTH));
            if (startCol + lenCols > BAR_WIDTH) {
                lenCols = Math.max(1, BAR_WIDTH - startCol);
            }

            char fillChar = switch (span.status()) {
                case "CACHE_HIT" -> '=';
                case "STALE_FALLBACK" -> '~';
                case "TIMEOUT", "ERROR", "CIRCUIT_OPEN", "BULKHEAD_REJECTED" -> '!';
                default -> '#';
            };

            StringBuilder bar = new StringBuilder(".".repeat(BAR_WIDTH));
            for (int i = startCol; i < startCol + lenCols; i++) {
                bar.setCharAt(i, fillChar);
            }

            String label = span.upstreamName().length() > 29
                ? span.upstreamName().substring(0, 29)
                : span.upstreamName();

            sb.append(String.format(
                "%-30s %-8s %-14s %7.2fms |%s|%n",
                label, span.protocol(), span.status(), span.durationMs(), bar
            ));
        }
        return sb.toString();
    }

    private static double round2(double val) {
        return Math.round(val * 100.0) / 100.0;
    }
}
