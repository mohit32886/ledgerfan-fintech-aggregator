package com.ledgerfan.aggregator.orchestrator;

import com.ledgerfan.aggregator.cache.ReadThroughCacheStore;
import com.ledgerfan.aggregator.domain.UpstreamCallOutcome;
import com.ledgerfan.aggregator.domain.UpstreamStatus;
import com.ledgerfan.aggregator.observability.SpanWaterfallEntry;
import com.ledgerfan.aggregator.upstreams.UpstreamClient;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Executes an upstream call inside an OpenTelemetry child span protected by:
 * 1. Redis two-tier read-through cache (fresh hit + stale-while-degraded fallback)
 * 2. Resilience4j CircuitBreaker + SemaphoreBulkhead + TimeLimiter
 * 3. Java 21 Virtual Threads
 */
@Component
public class ResilienceUpstreamExecutor {

    private static final String CACHE_NAMESPACE_PREFIX = "upstream:";

    private final ReadThroughCacheStore cacheStore;
    private final Tracer tracer;
    private final BulkheadRegistry bulkheadRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final TimeLimiterRegistry timeLimiterRegistry;
    private final Duration freshTtl;
    private final Duration staleTtl;
    private final ExecutorService virtualExecutor;

    public ResilienceUpstreamExecutor(
        ReadThroughCacheStore cacheStore,
        Tracer tracer,
        @Value("${ledgerfan.aggregator.cache-ttl-seconds:30}") long freshTtlSeconds,
        @Value("${ledgerfan.aggregator.stale-ttl-seconds:300}") long staleTtlSeconds
    ) {
        this.cacheStore = cacheStore;
        this.tracer = tracer;
        this.freshTtl = Duration.ofSeconds(freshTtlSeconds);
        this.staleTtl = Duration.ofSeconds(staleTtlSeconds);
        this.virtualExecutor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("vt-upstream-exec-", 0).factory()
        );

        this.bulkheadRegistry = BulkheadRegistry.of(
            BulkheadConfig.custom()
                .maxConcurrentCalls(64)
                .maxWaitDuration(Duration.ofMillis(5))
                .build()
        );
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(
            CircuitBreakerConfig.custom()
                .slidingWindowSize(20)
                .minimumNumberOfCalls(10)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(3)
                .build()
        );
        this.timeLimiterRegistry = TimeLimiterRegistry.ofDefaults();
    }

    public <T> UpstreamCallOutcome<T> execute(
        UpstreamClient<T> client,
        String accountId,
        boolean bypassFreshCache,
        Context parentContext,
        String rootSpanId,
        long rootStartNanos,
        Queue<SpanWaterfallEntry> recordedSpans
    ) {
        long startNanos = System.nanoTime();
        long startOffsetMicros = Math.max(0L, (startNanos - rootStartNanos) / 1_000L);
        String threadName = Thread.currentThread().toString();
        String cacheNamespace = cacheNamespace(client.name());

        Span span = tracer.spanBuilder("upstream.call." + client.name())
            .setParent(parentContext)
            .setSpanKind(SpanKind.CLIENT)
            .setAttribute("upstream.name", client.name())
            .setAttribute("upstream.protocol", client.protocol().name())
            .setAttribute("upstream.criticality", client.criticality().name())
            .setAttribute("account.id", accountId)
            .startSpan();

        UpstreamCallOutcome<T> outcome = null;
        try (Scope ignored = span.makeCurrent()) {
            if (!bypassFreshCache) {
                Optional<T> cached = cacheStore.getFresh(cacheNamespace, accountId, client.payloadType());
                if (cached.isPresent()) {
                    long durationMicros = elapsedMicros(startNanos);
                    span.setAttribute("cache.hit", true);
                    span.setAttribute("upstream.status", UpstreamStatus.CACHE_HIT.name());
                    outcome = new UpstreamCallOutcome<>(
                        client.name(),
                        client.protocol(),
                        client.criticality(),
                        UpstreamStatus.CACHE_HIT,
                        cached.get(),
                        durationMicros,
                        round2(durationMicros / 1000.0),
                        true,
                        false,
                        null,
                        threadName,
                        startOffsetMicros
                    );
                    return outcome;
                }
            }

            T payload = invokeWithResilience(client, accountId);
            cacheStore.put(cacheNamespace, accountId, payload, freshTtl, staleTtl);

            long durationMicros = elapsedMicros(startNanos);
            span.setAttribute("cache.hit", false);
            span.setAttribute("upstream.status", UpstreamStatus.SUCCESS.name());
            outcome = new UpstreamCallOutcome<>(
                client.name(),
                client.protocol(),
                client.criticality(),
                UpstreamStatus.SUCCESS,
                payload,
                durationMicros,
                round2(durationMicros / 1000.0),
                false,
                false,
                null,
                threadName,
                startOffsetMicros
            );
            return outcome;
        } catch (Throwable ex) {
            Throwable rootCause = unwrap(ex);
            UpstreamStatus failureStatus = classifyFailure(rootCause);
            span.setStatus(StatusCode.ERROR, rootCause.getMessage() != null ? rootCause.getMessage() : failureStatus.name());
            span.recordException(rootCause);

            Optional<T> stalePayload = cacheStore.getStaleFallback(cacheNamespace, accountId, client.payloadType());
            long durationMicros = elapsedMicros(startNanos);

            if (stalePayload.isPresent()) {
                span.setAttribute("cache.stale_fallback", true);
                span.setAttribute("upstream.status", UpstreamStatus.STALE_FALLBACK.name());
                outcome = new UpstreamCallOutcome<>(
                    client.name(),
                    client.protocol(),
                    client.criticality(),
                    UpstreamStatus.STALE_FALLBACK,
                    stalePayload.get(),
                    durationMicros,
                    round2(durationMicros / 1000.0),
                    true,
                    true,
                    "Served stale fallback after " + failureStatus.name() + ": " + rootCause.getMessage(),
                    threadName,
                    startOffsetMicros
                );
                return outcome;
            }

            span.setAttribute("upstream.status", failureStatus.name());
            outcome = new UpstreamCallOutcome<>(
                client.name(),
                client.protocol(),
                client.criticality(),
                failureStatus,
                null,
                durationMicros,
                round2(durationMicros / 1000.0),
                false,
                true,
                rootCause.getMessage(),
                threadName,
                startOffsetMicros
            );
            return outcome;
        } finally {
            if (outcome != null) {
                recordedSpans.add(new SpanWaterfallEntry(
                    span.getSpanContext().getSpanId(),
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
                    Map.of(
                        "criticality", outcome.criticality().name(),
                        "degraded", String.valueOf(outcome.degraded())
                    )
                ));
            }
            span.end();
        }
    }

    private <T> T invokeWithResilience(UpstreamClient<T> client, String accountId) throws Exception {
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(client.name());
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(client.name());
        TimeLimiter timeLimiter = timeLimiterRegistry.timeLimiter(
            client.name(),
            TimeLimiterConfig.custom()
                .timeoutDuration(client.slaTimeout())
                .cancelRunningFuture(true)
                .build()
        );

        Context currentOtelContext = Context.current();
        Supplier<T> timedFetch = () -> {
            try {
                return timeLimiter.executeFutureSupplier(() ->
                    CompletableFuture.supplyAsync(
                        currentOtelContext.wrapSupplier(() -> client.fetch(accountId)),
                        virtualExecutor
                    )
                );
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        };

        Supplier<T> bulkheadProtected = Bulkhead.decorateSupplier(bulkhead, timedFetch);
        Supplier<T> cbProtected = CircuitBreaker.decorateSupplier(circuitBreaker, bulkheadProtected);
        return cbProtected.get();
    }

    public static String cacheNamespace(String upstreamName) {
        return CACHE_NAMESPACE_PREFIX + upstreamName;
    }

    private static UpstreamStatus classifyFailure(Throwable ex) {
        if (ex instanceof TimeoutException || ex instanceof InterruptedException) {
            return UpstreamStatus.TIMEOUT;
        }
        if (ex instanceof CallNotPermittedException) {
            return UpstreamStatus.CIRCUIT_OPEN;
        }
        if (ex instanceof BulkheadFullException) {
            return UpstreamStatus.BULKHEAD_FULL;
        }
        return UpstreamStatus.ERROR;
    }

    private static Throwable unwrap(Throwable ex) {
        Throwable current = ex;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static long elapsedMicros(long startNanos) {
        return Math.max(1L, (System.nanoTime() - startNanos) / 1_000L);
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    @PreDestroy
    public void shutdown() {
        virtualExecutor.shutdownNow();
    }
}
