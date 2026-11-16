package com.ledgerfan.aggregator.benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgerfan.aggregator.outbox.LedgerPostingResult;
import com.ledgerfan.aggregator.outbox.LedgerPostingService;
import com.ledgerfan.aggregator.outbox.LedgerTransferRequest;
import com.ledgerfan.aggregator.outbox.OutboxEventEntity;
import com.ledgerfan.aggregator.outbox.OutboxEventRepository;
import com.ledgerfan.aggregator.outbox.OutboxRelayPublisher;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Executes a head-to-head workload benchmark comparing:
 * 1. Legacy Table-Level Debezium CDC (4 row-level topics with full before/after envelopes)
 * 2. Modern Transactional Outbox + Redis Resource Cache (1 compact semantic domain topic)
 */
@Service
public class CdcVsOutboxBenchmarkService {

    private static final int LEGACY_CDC_TOPICS = 4;
    private static final int LEGACY_PARTITIONS_PER_TOPIC = 12; // 48 partitions total
    private static final int MODERN_PARTITIONS_TOTAL = 24;     // 1 unified semantic topic

    private static final double BROKER_CONTROL_PLANE_FLOOR_USD = 450.0;
    private static final double MONTHLY_USD_PER_PARTITION_RF3 = 18.50;
    private static final long SUSTAINED_TX_PER_SECOND = 500L;
    private static final int RETENTION_DAYS = 7;
    private static final double REPLICATION_FACTOR = 3.0;
    private static final double MONTHLY_USD_PER_GB_STORED_AND_XFER = 0.13;

    private final LedgerPostingService postingService;
    private final OutboxRelayPublisher relayPublisher;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public CdcVsOutboxBenchmarkService(
        LedgerPostingService postingService,
        OutboxRelayPublisher relayPublisher,
        OutboxEventRepository outboxEventRepository,
        ObjectMapper objectMapper
    ) {
        this.postingService = postingService;
        this.relayPublisher = relayPublisher;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public BenchmarkComparisonReport runComparison(int requestedTransactions) {
        int txCount = Math.max(10, Math.min(requestedTransactions, 5000));

        // 1. Measure Legacy Table-Level Debezium CDC pipeline
        long legacyStartNanos = System.nanoTime();
        long legacyTotalEvents = 0;
        long legacyTotalBytes = 0;
        String sampleLegacyJson = null;

        for (int i = 0; i < txCount; i++) {
            String accountId = "ACC-BENCH-" + (i % 50);
            BigDecimal amount = BigDecimal.valueOf(125.50 + (i % 100));
            List<String> cdcEnvelopes = buildLegacyDebeziumCdcEnvelopes(accountId, amount, i);
            for (String envelope : cdcEnvelopes) {
                if (sampleLegacyJson == null) {
                    sampleLegacyJson = envelope;
                }
                legacyTotalEvents++;
                legacyTotalBytes += envelope.getBytes(StandardCharsets.UTF_8).length;
            }
        }
        double legacyElapsedMs = round2((System.nanoTime() - legacyStartNanos) / 1_000_000.0);

        // 2. Measure Modern Transactional Outbox + Redis Resource Cache pipeline
        long modernStartNanos = System.nanoTime();
        long modernTotalEvents = 0;
        long modernTotalBytes = 0;
        UUID sampleOutboxEventId = null;

        for (int i = 0; i < txCount; i++) {
            String accountId = "ACC-BENCH-" + (i % 50);
            LedgerPostingResult result = postingService.postTransaction(new LedgerTransferRequest(
                accountId,
                "CPTY-EXT-" + (i % 20),
                BigDecimal.valueOf(125.50 + (i % 100)),
                "GBP",
                (i % 2 == 0) ? "CREDIT" : "DEBIT",
                "BENCH-TX-" + i
            ));
            if (sampleOutboxEventId == null) {
                sampleOutboxEventId = result.outboxEventId();
            }
            modernTotalEvents++;
            modernTotalBytes += result.outboxPayloadBytes();
        }

        // Drain all outbox batches via the relay publisher
        OutboxRelayPublisher.OutboxRelayFlushResult flushResult;
        do {
            flushResult = relayPublisher.flushPendingEvents();
        } while (flushResult.remainingPendingCount() > 0);

        double modernElapsedMs = round2((System.nanoTime() - modernStartNanos) / 1_000_000.0);

        String sampleModernJson = sampleOutboxEventId == null
            ? "{}"
            : outboxEventRepository.findById(sampleOutboxEventId)
                .map(OutboxEventEntity::getPayloadJson)
                .orElse("{}");

        double legacyAvgBytes = legacyTotalEvents > 0 ? round2((double) legacyTotalBytes / legacyTotalEvents) : 0.0;
        double modernAvgBytes = modernTotalEvents > 0 ? round2((double) modernTotalBytes / modernTotalEvents) : 0.0;

        double eventCountReductionPct = round2(((double) (legacyTotalEvents - modernTotalEvents) / legacyTotalEvents) * 100.0);
        double payloadVolumeReductionPct = round2(((double) (legacyTotalBytes - modernTotalBytes) / legacyTotalBytes) * 100.0);

        int legacyPartitions = LEGACY_CDC_TOPICS * LEGACY_PARTITIONS_PER_TOPIC;
        int modernPartitions = MODERN_PARTITIONS_TOTAL;
        double partitionReductionPct = round2(((double) (legacyPartitions - modernPartitions) / legacyPartitions) * 100.0);

        double legacyBytesPerTx = (double) legacyTotalBytes / txCount;
        double modernBytesPerTx = (double) modernTotalBytes / txCount;

        double legacyMonthlyCost = estimateMonthlyKafkaCostUsd(legacyPartitions, legacyBytesPerTx);
        double modernMonthlyCost = estimateMonthlyKafkaCostUsd(modernPartitions, modernBytesPerTx);
        double costSavingsPct = round2(((legacyMonthlyCost - modernMonthlyCost) / legacyMonthlyCost) * 100.0);

        String notes = String.format(
            "Projected at %d avg TPS, %d-day retention, RF=%.0f: Legacy emits 4 Debezium table CDC events/tx across %d partitions; "
                + "Outbox emits 1 compact semantic event/tx across %d partitions with hot projections served from Redis.",
            SUSTAINED_TX_PER_SECOND, RETENTION_DAYS, REPLICATION_FACTOR, legacyPartitions, modernPartitions
        );

        return new BenchmarkComparisonReport(
            txCount,
            legacyTotalEvents,
            modernTotalEvents,
            eventCountReductionPct,
            legacyAvgBytes,
            modernAvgBytes,
            legacyTotalBytes,
            modernTotalBytes,
            payloadVolumeReductionPct,
            legacyPartitions,
            modernPartitions,
            partitionReductionPct,
            legacyMonthlyCost,
            modernMonthlyCost,
            costSavingsPct,
            legacyElapsedMs,
            modernElapsedMs,
            sampleLegacyJson,
            sampleModernJson,
            notes
        );
    }

    private double estimateMonthlyKafkaCostUsd(int partitions, double measuredBytesPerTx) {
        double partitionAndBrokerCost = BROKER_CONTROL_PLANE_FLOOR_USD + (partitions * MONTHLY_USD_PER_PARTITION_RF3);
        long txInRetentionWindow = SUSTAINED_TX_PER_SECOND * 86_400L * RETENTION_DAYS;
        double storedGb = (txInRetentionWindow * measuredBytesPerTx * REPLICATION_FACTOR) / (1024.0 * 1024.0 * 1024.0);
        double storageAndXferCost = storedGb * MONTHLY_USD_PER_GB_STORED_AND_XFER;
        return round2(partitionAndBrokerCost + storageAndXferCost);
    }

    private List<String> buildLegacyDebeziumCdcEnvelopes(String accountId, BigDecimal amount, int sequence) {
        long nowMs = Instant.now().toEpochMilli();
        String txId = "pg_tx_" + (9_000_000L + sequence);

        String accountsUpdate = serializeCdcEnvelope("monolith_accounts", "u", txId, nowMs,
            Map.of("account_id", accountId, "iban", "GB29BARC20038400001234", "available_balance", 10420.50,
                "booked_balance", 10420.50, "overdraft_limit", 1500.00, "tier", "PREMIER", "kyc_state", "VERIFIED",
                "lock_version", 41, "updated_by", "core-posting-worker"),
            Map.of("account_id", accountId, "iban", "GB29BARC20038400001234", "available_balance", 10420.50 + amount.doubleValue(),
                "booked_balance", 10420.50 + amount.doubleValue(), "overdraft_limit", 1500.00, "tier", "PREMIER", "kyc_state", "VERIFIED",
                "lock_version", 42, "updated_by", "core-posting-worker"));

        String journalInsert = serializeCdcEnvelope("monolith_ledger_journal", "c", txId, nowMs,
            null,
            Map.ofEntries(
                Map.entry("entry_id", UUID.randomUUID().toString()),
                Map.entry("account_id", accountId),
                Map.entry("counterparty_iban", "GB88NWBK60161331926819"),
                Map.entry("dr_cr_flag", "CR"),
                Map.entry("amount", amount),
                Map.entry("currency", "GBP"),
                Map.entry("fx_rate", 1.0000),
                Map.entry("value_date", "2026-11-17"),
                Map.entry("gl_cost_center", "UK-RETAIL-01"),
                Map.entry("posting_channel", "FPS_FASTER_PAYMENTS"),
                Map.entry("batch_seq", sequence)
            ));

        String snapshotUpdate = serializeCdcEnvelope("monolith_balance_snapshots", "u", txId, nowMs,
            Map.of("account_id", accountId, "snapshot_day", "2026-11-17", "opening_bal", 10000.00,
                "intraday_credits", 420.50, "intraday_debits", 0.00, "last_seq", sequence),
            Map.of("account_id", accountId, "snapshot_day", "2026-11-17", "opening_bal", 10000.00,
                "intraday_credits", 420.50 + amount.doubleValue(), "intraday_debits", 0.00, "last_seq", sequence + 1));

        String auditInsert = serializeCdcEnvelope("monolith_audit_trail", "c", txId, nowMs,
            null,
            Map.of("audit_id", UUID.randomUUID().toString(), "entity_name", "MONOLITH_ACCOUNTS", "entity_pk", accountId,
                "action", "BALANCE_MUTATION", "actor_service", "ledger-monolith-v2", "client_ip", "10.42.18.91",
                "compliance_tag", "PSD2_STRONG_AUTH", "correlation_id", UUID.randomUUID().toString()));

        return List.of(accountsUpdate, journalInsert, snapshotUpdate, auditInsert);
    }

    private String serializeCdcEnvelope(
        String tableName,
        String op,
        String txId,
        long tsMs,
        Map<String, Object> before,
        Map<String, Object> after
    ) {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("version", "2.5.4.Final");
        source.put("connector", "postgresql");
        source.put("name", "barclays_core_monolith");
        source.put("ts_ms", tsMs);
        source.put("snapshot", "false");
        source.put("db", "core_banking_prod");
        source.put("schema", "public");
        source.put("table", tableName);
        source.put("txId", txId);
        source.put("lsn", 2849102938L);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("before", before);
        payload.put("after", after);
        payload.put("source", source);
        payload.put("op", op);
        payload.put("ts_ms", tsMs);

        Map<String, Object> schema = Map.of(
            "type", "struct",
            "name", "barclays_core_monolith.public." + tableName + ".Envelope",
            "optional", false
        );

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("schema", schema);
        envelope.put("payload", payload);

        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize legacy CDC benchmark envelope", ex);
        }
    }

    private static double round2(double val) {
        return Math.round(val * 100.0) / 100.0;
    }
}
