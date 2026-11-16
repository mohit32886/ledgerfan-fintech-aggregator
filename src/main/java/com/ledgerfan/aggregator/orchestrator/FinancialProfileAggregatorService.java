package com.ledgerfan.aggregator.orchestrator;

import com.ledgerfan.aggregator.cache.ReadThroughCacheStore;
import com.ledgerfan.aggregator.domain.UpstreamCallOutcome;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.CheckingAccountSummary;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.CreditCardPortfolio;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.CustomerPreferences;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.DebitCardControls;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.FraudRiskSignals;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.FxTreasuryRates;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.GeneralLedgerPosition;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.InvestmentWealthSummary;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.KycComplianceProfile;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.LoanMortgageSummary;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.RewardsLoyaltySummary;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.StandingOrdersSummary;
import com.ledgerfan.aggregator.domain.UpstreamStatus;
import com.ledgerfan.aggregator.observability.InMemoryWaterfallExporter;
import com.ledgerfan.aggregator.observability.SpanWaterfallEntry;
import com.ledgerfan.aggregator.upstreams.UpstreamClient;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.concurrent.TimeoutException;

/**
 * Core sub-100ms 12-upstream financial profile aggregator built on Java 21
 * Virtual Threads + {@code StructuredTaskScope}, Resilience4j, Redis, and OpenTelemetry.
 */
@Service
@SuppressWarnings("preview")
public class FinancialProfileAggregatorService {

    private final List<UpstreamClient<?>> upstreams;
    private final ResilienceUpstreamExecutor upstreamExecutor;
    private final ReadThroughCacheStore cacheStore;
    private final Tracer tracer;
    private final InMemoryWaterfallExporter waterfallExporter;
    private final long globalDeadlineMs;

    public FinancialProfileAggregatorService(
        List<UpstreamClient<?>> upstreams,
        ResilienceUpstreamExecutor upstreamExecutor,
        ReadThroughCacheStore cacheStore,
        Tracer tracer,
        InMemoryWaterfallExporter waterfallExporter,
        @Value("${ledgerfan.aggregator.global-deadline-ms:85}") long globalDeadlineMs
    ) {
        this.upstreams = upstreams;
        this.upstreamExecutor = upstreamExecutor;
        this.cacheStore = cacheStore;
        this.tracer = tracer;
        this.waterfallExporter = waterfallExporter;
        this.globalDeadlineMs = globalDeadlineMs;
    }

    public FinancialProfileResponse aggregateProfile(String accountId, boolean bypassFreshCache) {
        long rootStartNanos = System.nanoTime();
        Instant deadline = Instant.now().plusMillis(globalDeadlineMs);

        Span rootSpan = tracer.spanBuilder("aggregator.financial_profile.fanout")
            .setSpanKind(SpanKind.SERVER)
            .setAttribute("account.id", accountId)
            .setAttribute("upstreams.count", upstreams.size())
            .setAttribute("sla.deadline_ms", globalDeadlineMs)
            .setAttribute("cache.bypass_fresh", bypassFreshCache)
            .startSpan();

        String traceId = rootSpan.getSpanContext().getTraceId();
        String rootSpanId = rootSpan.getSpanContext().getSpanId();
        Queue<SpanWaterfallEntry> recordedSpans = new ConcurrentLinkedQueue<>();
        Map<String, UpstreamCallOutcome<?>> outcomeByUpstream = new ConcurrentHashMap<>();

        try (Scope ignored = rootSpan.makeCurrent()) {
            Context parentOtelContext = Context.current();
            Map<UpstreamClient<?>, Subtask<UpstreamCallOutcome<?>>> subtasks = new LinkedHashMap<>();

            try (var scope = new PartialDegradationTaskScope(parentOtelContext)) {
                for (UpstreamClient<?> client : upstreams) {
                    Subtask<UpstreamCallOutcome<?>> subtask = scope.forkWithOtelContext(() ->
                        upstreamExecutor.execute(
                            client,
                            accountId,
                            bypassFreshCache,
                            parentOtelContext,
                            rootSpanId,
                            rootStartNanos,
                            recordedSpans
                        )
                    );
                    subtasks.put(client, subtask);
                }

                try {
                    scope.joinUntil(deadline);
                } catch (TimeoutException timeoutEx) {
                    scope.shutdown();
                    rootSpan.setAttribute("aggregator.global_deadline_exceeded", true);
                } catch (InterruptedException interruptedEx) {
                    Thread.currentThread().interrupt();
                    scope.shutdown();
                }

                for (Map.Entry<UpstreamClient<?>, Subtask<UpstreamCallOutcome<?>>> entry : subtasks.entrySet()) {
                    UpstreamClient<?> client = entry.getKey();
                    Subtask<UpstreamCallOutcome<?>> subtask = entry.getValue();

                    if (subtask.state() == Subtask.State.SUCCESS && subtask.get() != null) {
                        outcomeByUpstream.put(client.name(), subtask.get());
                    } else {
                        UpstreamCallOutcome<?> fallbackOutcome = degradeUnfinishedUpstream(client, accountId, rootStartNanos);
                        outcomeByUpstream.put(client.name(), fallbackOutcome);
                    }
                }
            }

            long totalMicros = Math.max(1L, (System.nanoTime() - rootStartNanos) / 1_000L);
            double totalMs = round2(totalMicros / 1000.0);

            List<SpanWaterfallEntry> allSpans = new ArrayList<>(recordedSpans);
            ensureAllUpstreamsInWaterfall(allSpans, outcomeByUpstream, rootSpanId);
            allSpans.addFirst(new SpanWaterfallEntry(
                rootSpanId,
                null,
                "aggregator.financial_profile.fanout",
                "aggregator-root",
                "INTERNAL",
                "SUCCESS",
                0L,
                totalMicros,
                totalMs,
                Thread.currentThread().toString(),
                false,
                Map.of("upstreams", String.valueOf(upstreams.size()))
            ));

            waterfallExporter.record(traceId, rootSpanId, accountId, totalMicros, globalDeadlineMs, allSpans);

            return buildResponse(accountId, traceId, totalMs, outcomeByUpstream);
        } finally {
            rootSpan.end();
        }
    }

    private <T> UpstreamCallOutcome<T> degradeUnfinishedUpstream(
        UpstreamClient<T> client,
        String accountId,
        long rootStartNanos
    ) {
        long latencyMicros = Math.max(1L, (System.nanoTime() - rootStartNanos) / 1_000L);
        String namespace = ResilienceUpstreamExecutor.cacheNamespace(client.name());
        Optional<T> stale = cacheStore.getStaleFallback(namespace, accountId, client.payloadType());
        if (stale.isPresent()) {
            return new UpstreamCallOutcome<>(
                client.name(),
                client.protocol(),
                client.criticality(),
                UpstreamStatus.STALE_FALLBACK,
                stale.get(),
                latencyMicros,
                round2(latencyMicros / 1000.0),
                true,
                true,
                "Global deadline reached; served stale cache fallback",
                "vt-fanout-deadline",
                0L
            );
        }

        return new UpstreamCallOutcome<>(
            client.name(),
            client.protocol(),
            client.criticality(),
            UpstreamStatus.TIMEOUT,
            null,
            latencyMicros,
            round2(latencyMicros / 1000.0),
            false,
            true,
            "Global deadline (" + globalDeadlineMs + "ms) exceeded before upstream responded",
            "vt-fanout-deadline",
            0L
        );
    }

    private void ensureAllUpstreamsInWaterfall(
        List<SpanWaterfallEntry> spans,
        Map<String, UpstreamCallOutcome<?>> outcomeByUpstream,
        String rootSpanId
    ) {
        for (UpstreamClient<?> client : upstreams) {
            boolean alreadyRecorded = spans.stream().anyMatch(s -> client.name().equals(s.upstreamName()));
            if (!alreadyRecorded) {
                UpstreamCallOutcome<?> outcome = outcomeByUpstream.get(client.name());
                if (outcome != null) {
                    spans.add(new SpanWaterfallEntry(
                        "deadline-" + client.name(),
                        rootSpanId,
                        "upstream.call." + client.name(),
                        client.name(),
                        client.protocol().name(),
                        outcome.status().name(),
                        outcome.startOffsetMicros(),
                        outcome.latencyMicros(),
                        outcome.latencyMs(),
                        outcome.virtualThreadName(),
                        outcome.cacheHit(),
                        Map.of("degraded", String.valueOf(outcome.degraded()))
                    ));
                }
            }
        }
    }

    private FinancialProfileResponse buildResponse(
        String accountId,
        String traceId,
        double totalMs,
        Map<String, UpstreamCallOutcome<?>> outcomes
    ) {
        int succeededCount = 0;
        int cacheHitCount = 0;
        int staleFallbackCount = 0;
        int degradedCount = 0;
        double sumSequentialMs = 0.0;
        List<String> degradedUpstreams = new ArrayList<>();
        Map<String, String> statuses = new LinkedHashMap<>();
        Map<String, Double> timings = new LinkedHashMap<>();

        for (UpstreamClient<?> client : upstreams) {
            UpstreamCallOutcome<?> outcome = outcomes.get(client.name());
            if (outcome == null) {
                continue;
            }
            statuses.put(client.name(), outcome.status().name());
            timings.put(client.name(), outcome.latencyMs());
            sumSequentialMs += outcome.latencyMs();

            if (outcome.status() == UpstreamStatus.CACHE_HIT) {
                cacheHitCount++;
                succeededCount++;
            } else if (outcome.status() == UpstreamStatus.SUCCESS) {
                succeededCount++;
            } else if (outcome.status() == UpstreamStatus.STALE_FALLBACK) {
                staleFallbackCount++;
                degradedCount++;
                degradedUpstreams.add(client.name());
            } else {
                degradedCount++;
                degradedUpstreams.add(client.name());
            }
        }

        double roundedSeq = round2(sumSequentialMs);
        double speedup = totalMs > 0 ? round2(roundedSeq / totalMs) : 1.0;
        String overallStatus = degradedCount == 0 ? "COMPLETE" : "PARTIAL_DEGRADED";

        AggregationMetadata metadata = new AggregationMetadata(
            accountId,
            traceId,
            overallStatus,
            totalMs,
            roundedSeq,
            speedup,
            globalDeadlineMs,
            totalMs <= 100.0,
            upstreams.size(),
            succeededCount,
            cacheHitCount,
            staleFallbackCount,
            degradedCount,
            degradedUpstreams,
            statuses,
            timings
        );

        return new FinancialProfileResponse(
            metadata,
            payload(outcomes, "checking-accounts-rest", CheckingAccountSummary.class),
            payload(outcomes, "kyc-compliance-graphql", KycComplianceProfile.class),
            payload(outcomes, "general-ledger-rest", GeneralLedgerPosition.class),
            payload(outcomes, "credit-cards-graphql", CreditCardPortfolio.class),
            payload(outcomes, "debit-card-controls-rest", DebitCardControls.class),
            payload(outcomes, "fraud-risk-rest", FraudRiskSignals.class),
            payload(outcomes, "loan-mortgage-graphql", LoanMortgageSummary.class),
            payload(outcomes, "investment-wealth-rest", InvestmentWealthSummary.class),
            payload(outcomes, "rewards-loyalty-graphql", RewardsLoyaltySummary.class),
            payload(outcomes, "standing-orders-rest", StandingOrdersSummary.class),
            payload(outcomes, "fx-treasury-rest", FxTreasuryRates.class),
            payload(outcomes, "customer-preferences-graphql", CustomerPreferences.class)
        );
    }

    private static <T> T payload(
        Map<String, UpstreamCallOutcome<?>> outcomes,
        String upstreamName,
        Class<T> type
    ) {
        UpstreamCallOutcome<?> outcome = outcomes.get(upstreamName);
        if (outcome == null || outcome.payload() == null) {
            return null;
        }
        return type.cast(outcome.payload());
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
