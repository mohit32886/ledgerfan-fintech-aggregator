package com.ledgerfan.aggregator.api;

import com.ledgerfan.aggregator.cache.ReadThroughCacheStore;
import com.ledgerfan.aggregator.upstreams.UpstreamSimulationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class FinancialProfileControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private UpstreamSimulationRegistry simulationRegistry;

    @Autowired
    private ReadThroughCacheStore cacheStore;

    @BeforeEach
    void resetState() {
        simulationRegistry.resetAll();
        cacheStore.clearAll();
    }

    @Test
    void servesAggregatedProfileWaterfallOutboxAndBenchmarkEndpoints() throws Exception {
        // Warm up profile endpoint once, then verify complete aggregation under 100ms
        mockMvc.perform(get("/api/v1/accounts/ACC-INT-001/financial-profile").param("bypassCache", "true"))
            .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/accounts/ACC-INT-001/financial-profile").param("bypassCache", "true"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.metadata.accountId", is("ACC-INT-001")))
            .andExpect(jsonPath("$.metadata.status", is("COMPLETE")))
            .andExpect(jsonPath("$.metadata.totalUpstreams", is(12)))
            .andExpect(jsonPath("$.metadata.totalElapsedMs", lessThan(100.0)))
            .andExpect(jsonPath("$.checkingAccount.currency", is("GBP")))
            .andExpect(jsonPath("$.kycCompliance.sanctionsCleared", is(true)));

        // Verify partial degradation via query parameter fault injection
        mockMvc.perform(get("/api/v1/accounts/ACC-INT-002/financial-profile")
                .param("bypassCache", "true")
                .param("simulateSlowUpstreams", "loan-mortgage-graphql:180"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.metadata.status", is("PARTIAL_DEGRADED")))
            .andExpect(jsonPath("$.metadata.degradedUpstreams", hasItem("loan-mortgage-graphql")))
            .andExpect(jsonPath("$.metadata.totalElapsedMs", lessThan(100.0)));

        // Verify OpenTelemetry waterfall endpoint
        mockMvc.perform(get("/api/v1/accounts/ACC-INT-001/financial-profile/waterfall"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.accountId", is("ACC-INT-001")))
            .andExpect(jsonPath("$.spans.length()", is(13)))
            .andExpect(jsonPath("$.asciiWaterfall", containsString("Trace Waterfall")));

        // Verify simulation configure & reset endpoints
        mockMvc.perform(post("/api/v1/upstreams/simulation")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {"artificialLatencyMs":{"fx-treasury-rest":190},"failureProbability":{}}
                    """))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.artificialLatencyMs['fx-treasury-rest']", is(190)));

        mockMvc.perform(post("/api/v1/upstreams/simulation/reset"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.artificialLatencyMs").isEmpty());

        // Verify transactional outbox posting & events inspection
        String transferBody = """
            {
              "accountId": "ACC-INT-001",
              "counterpartyAccountId": "CPTY-EXT-42",
              "amount": 175.25,
              "currency": "GBP",
              "transactionType": "CREDIT",
              "reference": "API-INT-TEST"
            }
            """;

        mockMvc.perform(post("/api/v1/ledger/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content(transferBody))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.posting.accountId", is("ACC-INT-001")))
            .andExpect(jsonPath("$.relayFlush.publishedCount", greaterThan(0)))
            .andExpect(jsonPath("$.redisResourceProjection.accountId", is("ACC-INT-001")));

        mockMvc.perform(get("/api/v1/outbox/events"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.publishedOutboxRows", greaterThan(0)));

        // Verify CDC vs Outbox benchmark endpoint
        mockMvc.perform(post("/api/v1/benchmarks/cdc-vs-outbox").param("transactions", "250"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.transactionsProcessed", is(250)))
            .andExpect(jsonPath("$.legacyCdcTotalEvents", is(1000)))
            .andExpect(jsonPath("$.modernOutboxTotalEvents", is(250)))
            .andExpect(jsonPath("$.estimatedCostSavingsPercent", greaterThan(40.0)))
            .andExpect(jsonPath("$.estimatedCostSavingsPercent", lessThan(60.0)));
    }
}
