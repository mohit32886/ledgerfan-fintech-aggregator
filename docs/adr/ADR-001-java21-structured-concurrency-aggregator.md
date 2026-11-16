# ADR-001: Decomposing Monolith Account Reads into a Sub-100ms Java 21 Structured Concurrency Aggregator

- **Status:** Accepted
- **Date:** 2026-11-17
- **Component:** `com.ledgerfan.aggregator.orchestrator`

## Context

Core retail and wealth banking profiles require assembling live data across 12 distinct downstream capabilities spanning both REST and GraphQL services:

1. `checking-accounts-rest` (`REST`, `CRITICAL`)
2. `kyc-compliance-graphql` (`GRAPHQL`, `CRITICAL`)
3. `general-ledger-rest` (`REST`, `CRITICAL`)
4. `credit-cards-graphql` (`GRAPHQL`, `IMPORTANT`)
5. `debit-card-controls-rest` (`REST`, `IMPORTANT`)
6. `fraud-risk-rest` (`REST`, `CRITICAL`)
7. `loan-mortgage-graphql` (`GRAPHQL`, `DEGRADABLE`)
8. `investment-wealth-rest` (`REST`, `DEGRADABLE`)
9. `rewards-loyalty-graphql` (`GRAPHQL`, `DEGRADABLE`)
10. `standing-orders-rest` (`REST`, `DEGRADABLE`)
11. `fx-treasury-rest` (`REST`, `DEGRADABLE`)
12. `customer-preferences-graphql` (`GRAPHQL`, `DEGRADABLE`)

Sequential execution across these 12 upstreams accumulates >218 ms of pure network latency under healthy conditions and spikes above 1,000 ms whenever a single non-critical GraphQL upstream experiences tail latency. Legacy fixed-size Tomcat/ForkJoin thread pools also suffered thread starvation during downstream stalls.

## Decision

We adopt a **Java 21 Virtual Threads + `StructuredTaskScope`** fan-out orchestrator (`FinancialProfileAggregatorService`) backed by **Resilience4j** and a **two-tier Redis read-through cache**:

1. **Non-Blocking Structured Fan-Out (`PartialDegradationTaskScope`)**
   - Extends `java.util.concurrent.StructuredTaskScope<UpstreamCallOutcome<?>>` using a named virtual thread factory (`vt-fanout-`).
   - Unlike `ShutdownOnFailure`, `PartialDegradationTaskScope` isolates individual upstream failures and timeouts so healthy sibling upstreams always complete.
   - Enforces a hard `global-deadline-ms: 85` budget via `scope.joinUntil(deadline)`, guaranteeing sub-100 ms API response SLAs.

2. **Per-Upstream Bulkheads, Circuit Breakers & Time Limiters (`ResilienceUpstreamExecutor`)**
   - **Semaphore Bulkhead** (`maxConcurrentCalls=64`, `maxWaitDuration=5ms`) prevents any single slow upstream from saturating shared resources.
   - **TimeLimiter** (`per-upstream-timeout-ms: 60`, `cancelRunningFuture=true`) terminates runaway upstream calls early and interrupts the underlying virtual thread.
   - **CircuitBreaker** (`slidingWindowSize=20`, `failureRateThreshold=50%`, `waitDurationInOpenState=10s`) fast-fails persistently unhealthy services.

3. **Two-Tier Redis Read-Through + Stale-While-Degraded Cache (`ReadThroughCacheStore`)**
   - **Tier 1 (Fresh TTL: 30s):** Serves repeated reads in sub-millisecond latency (`CACHE_HIT`).
   - **Tier 2 (Stale Fallback TTL: 300s):** If an upstream times out, opens its circuit, or hits the global deadline, `ResilienceUpstreamExecutor` serves the last-known-good snapshot flagged as `STALE_FALLBACK` (`degraded=true`) rather than dropping data.

4. **W3C Trace Context Propagation Across Virtual Threads (`TracingConfig` & `InMemoryWaterfallExporter`)**
   - `PartialDegradationTaskScope.forkWithOtelContext(...)` wraps every virtual-thread `Callable` with `io.opentelemetry.context.Context.wrap(...)` so all 12 child spans link directly to the root aggregation `traceId` and `parentSpanId`, exporting to Jaeger/Tempo over OTLP HTTP (`4318`) and rendering live execution waterfalls.

## Consequences

- **Positive:** P99 fan-out latency drops from >218 ms sequential to ~30–40 ms concurrent (>5x speedup) while sustaining full availability (`PARTIAL_DEGRADED`) during partial outages.
- **Tradeoff:** `StructuredTaskScope` (JEP 453) requires `--enable-preview` at compile and runtime on JDK 21 until finalized in later JDK releases.
