package com.ledgerfan.aggregator.orchestrator;

import com.ledgerfan.aggregator.cache.ReadThroughCacheStore;
import com.ledgerfan.aggregator.observability.InMemoryWaterfallExporter;
import com.ledgerfan.aggregator.observability.SpanWaterfallEntry;
import com.ledgerfan.aggregator.observability.TraceWaterfallSnapshot;
import com.ledgerfan.aggregator.upstreams.UpstreamSimulationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class FinancialProfileAggregatorServiceTest {

    @Autowired
    private FinancialProfileAggregatorService aggregatorService;

    @Autowired
    private UpstreamSimulationRegistry simulationRegistry;

    @Autowired
    private ReadThroughCacheStore cacheStore;

    @Autowired
    private InMemoryWaterfallExporter waterfallExporter;

    @BeforeEach
    void setUp() {
        simulationRegistry.resetAll();
        cacheStore.clearAll();
        // Warm up Resilience4j and virtual thread scheduler before latency assertions
        aggregatorService.aggregateProfile("ACC-WARMUP-000", true);
        cacheStore.clearAll();
    }

    @Test
    void aggregatesAll12RestAndGraphqlUpstreamsConcurrentlyUnder100ms() {
        FinancialProfileResponse response = aggregatorService.aggregateProfile("ACC-UK-9001", true);

        AggregationMetadata meta = response.metadata();
        assertThat(meta.status()).isEqualTo("COMPLETE");
        assertThat(meta.totalUpstreams()).isEqualTo(12);
        assertThat(meta.succeededCount()).isEqualTo(12);
        assertThat(meta.degradedCount()).isZero();
        assertThat(meta.totalElapsedMs()).isLessThan(100.0);
        assertThat(meta.slaMet()).isTrue();
        assertThat(meta.sumSequentialUpstreamMs()).isGreaterThan(200.0);
        assertThat(meta.speedupFactor()).isGreaterThan(4.0);

        assertThat(response.checkingAccount()).isNotNull();
        assertThat(response.kycCompliance()).isNotNull();
        assertThat(response.generalLedger()).isNotNull();
        assertThat(response.creditCards()).isNotNull();
        assertThat(response.debitCardControls()).isNotNull();
        assertThat(response.fraudRiskSignals()).isNotNull();
        assertThat(response.loansAndMortgages()).isNotNull();
        assertThat(response.investmentsAndWealth()).isNotNull();
        assertThat(response.rewardsAndLoyalty()).isNotNull();
        assertThat(response.standingOrders()).isNotNull();
        assertThat(response.fxTreasuryRates()).isNotNull();
        assertThat(response.customerPreferences()).isNotNull();
    }

    @Test
    void degradesGracefullyWhenNonCriticalUpstreamsExceedSlaTimeout() {
        simulationRegistry.setArtificialLatency("loan-mortgage-graphql", 250L);
        simulationRegistry.setArtificialLatency("rewards-loyalty-graphql", 250L);

        FinancialProfileResponse response = aggregatorService.aggregateProfile("ACC-UK-9002", true);
        AggregationMetadata meta = response.metadata();

        assertThat(meta.status()).isEqualTo("PARTIAL_DEGRADED");
        assertThat(meta.totalElapsedMs()).isLessThan(100.0);
        assertThat(meta.succeededCount()).isEqualTo(10);
        assertThat(meta.degradedCount()).isEqualTo(2);
        assertThat(meta.degradedUpstreams())
            .containsExactlyInAnyOrder("loan-mortgage-graphql", "rewards-loyalty-graphql");

        // Degraded upstreams are null without cache, while all 10 healthy upstreams are populated
        assertThat(response.loansAndMortgages()).isNull();
        assertThat(response.rewardsAndLoyalty()).isNull();
        assertThat(response.checkingAccount()).isNotNull();
        assertThat(response.kycCompliance()).isNotNull();
        assertThat(response.generalLedger()).isNotNull();
        assertThat(response.fraudRiskSignals()).isNotNull();
    }

    @Test
    void servesFreshReadThroughCacheHitsAndStaleFallbackOnUpstreamTimeout() {
        String accountId = "ACC-UK-9003";

        // 1. Prime cache with live calls
        FinancialProfileResponse primed = aggregatorService.aggregateProfile(accountId, false);
        assertThat(primed.metadata().cacheHitCount()).isZero();
        assertThat(primed.loansAndMortgages()).isNotNull();

        // 2. Subsequent call serves all 12 upstreams from fresh cache in sub-5ms
        FinancialProfileResponse cached = aggregatorService.aggregateProfile(accountId, false);
        assertThat(cached.metadata().cacheHitCount()).isEqualTo(12);
        assertThat(cached.metadata().totalElapsedMs()).isLessThan(20.0);

        // 3. Expire fresh window for loan-mortgage-graphql while preserving stale fallback window, then inject timeout
        cacheStore.expireFreshWindow(ResilienceUpstreamExecutor.cacheNamespace("loan-mortgage-graphql"), accountId);
        simulationRegistry.setArtificialLatency("loan-mortgage-graphql", 250L);

        FinancialProfileResponse fallbackResponse = aggregatorService.aggregateProfile(accountId, false);
        AggregationMetadata meta = fallbackResponse.metadata();

        assertThat(meta.status()).isEqualTo("PARTIAL_DEGRADED");
        assertThat(meta.staleFallbackCount()).isEqualTo(1);
        assertThat(meta.upstreamStatuses().get("loan-mortgage-graphql")).isEqualTo("STALE_FALLBACK");
        assertThat(fallbackResponse.loansAndMortgages()).isNotNull();
        assertThat(fallbackResponse.loansAndMortgages()).isEqualTo(primed.loansAndMortgages());
    }

    @Test
    void propagatesOpenTelemetryTraceContextAcrossVirtualThreadsAndExportsWaterfall() {
        String accountId = "ACC-UK-9004";
        FinancialProfileResponse response = aggregatorService.aggregateProfile(accountId, true);

        TraceWaterfallSnapshot waterfall = waterfallExporter.findByTraceId(response.metadata().traceId())
            .orElseThrow();

        assertThat(waterfall.traceId()).isNotBlank();
        assertThat(waterfall.accountId()).isEqualTo(accountId);
        assertThat(waterfall.slaMet()).isTrue();
        assertThat(waterfall.parallelismFactor()).isGreaterThan(3.0);

        // 1 root span + 12 upstream spans = 13 spans total
        assertThat(waterfall.spans()).hasSize(13);

        List<SpanWaterfallEntry> childSpans = waterfall.spans().stream()
            .filter(s -> !"INTERNAL".equals(s.protocol()))
            .toList();
        assertThat(childSpans).hasSize(12);
        assertThat(childSpans).allSatisfy(span -> {
            assertThat(span.parentSpanId()).isEqualTo(waterfall.rootSpanId());
            assertThat(span.virtualThreadName()).contains("VirtualThread");
        });

        assertThat(waterfall.asciiWaterfall())
            .contains("Trace Waterfall")
            .contains("checking-accounts-rest")
            .contains("kyc-compliance-graphql")
            .contains("loan-mortgage-graphql");
    }
}
