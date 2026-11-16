package com.ledgerfan.aggregator.upstreams;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Runtime registry governing simulated network latency, jitter, and chaos/fault injection
 * across the 12 downstream banking services.
 */
@Component
public class UpstreamSimulationRegistry {

    private final Map<String, Long> baseLatencyMs = new ConcurrentHashMap<>();
    private final Map<String, Long> artificialLatencyMs = new ConcurrentHashMap<>();
    private final Map<String, Double> failureProbability = new ConcurrentHashMap<>();

    public UpstreamSimulationRegistry() {
        registerDefaults();
    }

    public final void registerDefaults() {
        baseLatencyMs.clear();
        artificialLatencyMs.clear();
        failureProbability.clear();

        baseLatencyMs.put("checking-accounts-rest", 16L);
        baseLatencyMs.put("kyc-compliance-graphql", 22L);
        baseLatencyMs.put("general-ledger-rest", 19L);
        baseLatencyMs.put("credit-cards-graphql", 24L);
        baseLatencyMs.put("debit-card-controls-rest", 14L);
        baseLatencyMs.put("fraud-risk-rest", 18L);
        baseLatencyMs.put("loan-mortgage-graphql", 26L);
        baseLatencyMs.put("investment-wealth-rest", 21L);
        baseLatencyMs.put("rewards-loyalty-graphql", 20L);
        baseLatencyMs.put("standing-orders-rest", 15L);
        baseLatencyMs.put("fx-treasury-rest", 12L);
        baseLatencyMs.put("customer-preferences-graphql", 11L);
    }

    public void setArtificialLatency(String upstreamName, long latencyMs) {
        if (latencyMs <= 0) {
            artificialLatencyMs.remove(upstreamName);
        } else {
            artificialLatencyMs.put(upstreamName, latencyMs);
        }
    }

    public void setFailureProbability(String upstreamName, double probability) {
        if (probability <= 0.0) {
            failureProbability.remove(upstreamName);
        } else {
            failureProbability.put(upstreamName, Math.min(1.0, probability));
        }
    }

    public void resetAll() {
        registerDefaults();
    }

    public Map<String, Long> currentArtificialLatencies() {
        return Collections.unmodifiableMap(artificialLatencyMs);
    }

    public Map<String, Double> currentFailureProbabilities() {
        return Collections.unmodifiableMap(failureProbability);
    }

    public void simulateCall(String upstreamName) {
        long latency = artificialLatencyMs.containsKey(upstreamName)
            ? artificialLatencyMs.get(upstreamName)
            : baseLatencyMs.getOrDefault(upstreamName, 15L) + ThreadLocalRandom.current().nextLong(0, 5);

        if (latency > 0) {
            try {
                Thread.sleep(latency);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while awaiting upstream " + upstreamName, ex);
            }
        }

        double failProb = failureProbability.getOrDefault(upstreamName, 0.0);
        if (failProb > 0.0 && (failProb >= 1.0 || ThreadLocalRandom.current().nextDouble() < failProb)) {
            throw new SimulatedUpstreamException("Simulated upstream failure from " + upstreamName);
        }
    }

    public static class SimulatedUpstreamException extends RuntimeException {
        public SimulatedUpstreamException(String message) {
            super(message);
        }
    }
}
