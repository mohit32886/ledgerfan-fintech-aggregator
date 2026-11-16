package com.ledgerfan.aggregator.api;

import com.ledgerfan.aggregator.cache.CacheStatsSnapshot;
import com.ledgerfan.aggregator.cache.ReadThroughCacheStore;
import com.ledgerfan.aggregator.observability.InMemoryWaterfallExporter;
import com.ledgerfan.aggregator.observability.TraceWaterfallSnapshot;
import com.ledgerfan.aggregator.orchestrator.FinancialProfileAggregatorService;
import com.ledgerfan.aggregator.orchestrator.FinancialProfileResponse;
import com.ledgerfan.aggregator.upstreams.UpstreamSimulationRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * REST API exposing the sub-100ms 12-upstream financial profile aggregator,
 * OpenTelemetry trace waterfall inspection, and upstream chaos/latency simulation.
 */
@RestController
@RequestMapping("/api/v1")
public class FinancialProfileController {

    private final FinancialProfileAggregatorService aggregatorService;
    private final InMemoryWaterfallExporter waterfallExporter;
    private final UpstreamSimulationRegistry simulationRegistry;
    private final ReadThroughCacheStore cacheStore;

    public FinancialProfileController(
        FinancialProfileAggregatorService aggregatorService,
        InMemoryWaterfallExporter waterfallExporter,
        UpstreamSimulationRegistry simulationRegistry,
        ReadThroughCacheStore cacheStore
    ) {
        this.aggregatorService = aggregatorService;
        this.waterfallExporter = waterfallExporter;
        this.simulationRegistry = simulationRegistry;
        this.cacheStore = cacheStore;
    }

    @GetMapping("/accounts/{accountId}/financial-profile")
    public FinancialProfileResponse getFinancialProfile(
        @PathVariable String accountId,
        @RequestParam(defaultValue = "false") boolean bypassCache,
        @RequestParam(required = false) String simulateSlowUpstreams
    ) {
        Map<String, Long> previousOverrides = new HashMap<>();
        boolean appliedTransientSimulation = false;

        if (simulateSlowUpstreams != null && !simulateSlowUpstreams.isBlank()) {
            previousOverrides.putAll(simulationRegistry.currentArtificialLatencies());
            for (String token : simulateSlowUpstreams.split(",")) {
                String[] parts = token.trim().split(":");
                if (parts.length == 2) {
                    simulationRegistry.setArtificialLatency(parts[0].trim(), Long.parseLong(parts[1].trim()));
                    appliedTransientSimulation = true;
                }
            }
        }

        try {
            return aggregatorService.aggregateProfile(accountId, bypassCache);
        } finally {
            if (appliedTransientSimulation) {
                for (String key : simulationRegistry.currentArtificialLatencies().keySet()) {
                    simulationRegistry.setArtificialLatency(key, previousOverrides.getOrDefault(key, 0L));
                }
                for (Map.Entry<String, Long> prev : previousOverrides.entrySet()) {
                    simulationRegistry.setArtificialLatency(prev.getKey(), prev.getValue());
                }
            }
        }
    }

    @GetMapping("/accounts/{accountId}/financial-profile/waterfall")
    public ResponseEntity<TraceWaterfallSnapshot> getLatestWaterfall(@PathVariable String accountId) {
        return waterfallExporter.findLatestByAccountId(accountId)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/traces/{traceId}/waterfall")
    public ResponseEntity<TraceWaterfallSnapshot> getWaterfallByTraceId(@PathVariable String traceId) {
        return waterfallExporter.findByTraceId(traceId)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/cache/stats")
    public CacheStatsSnapshot cacheStats() {
        return cacheStore.stats();
    }

    @PostMapping("/upstreams/simulation")
    public UpstreamSimulationState configureSimulation(@RequestBody UpstreamSimulationRequest request) {
        if (request.artificialLatencyMs() != null) {
            request.artificialLatencyMs().forEach(simulationRegistry::setArtificialLatency);
        }
        if (request.failureProbability() != null) {
            request.failureProbability().forEach(simulationRegistry::setFailureProbability);
        }
        return new UpstreamSimulationState(
            simulationRegistry.currentArtificialLatencies(),
            simulationRegistry.currentFailureProbabilities()
        );
    }

    @PostMapping("/upstreams/simulation/reset")
    public UpstreamSimulationState resetSimulation() {
        simulationRegistry.resetAll();
        return new UpstreamSimulationState(
            simulationRegistry.currentArtificialLatencies(),
            simulationRegistry.currentFailureProbabilities()
        );
    }

    public record UpstreamSimulationRequest(
        Map<String, Long> artificialLatencyMs,
        Map<String, Double> failureProbability
    ) {
    }

    public record UpstreamSimulationState(
        Map<String, Long> artificialLatencyMs,
        Map<String, Double> failureProbability
    ) {
    }
}
