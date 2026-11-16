package com.ledgerfan.aggregator.benchmark;

/**
 * Quantitative comparison report contrasting legacy multi-table Debezium CDC topics
 * against the Transactional Outbox + Redis Resource Cache event mesh.
 */
public record BenchmarkComparisonReport(
    int transactionsProcessed,
    long legacyCdcTotalEvents,
    long modernOutboxTotalEvents,
    double eventCountReductionPercent,
    double legacyCdcAvgEventBytes,
    double modernOutboxAvgEventBytes,
    long legacyCdcTotalBytes,
    long modernOutboxTotalBytes,
    double payloadVolumeReductionPercent,
    int legacyPartitionsRequired,
    int modernPartitionsRequired,
    double partitionReductionPercent,
    double estimatedMonthlyKafkaCostUsdLegacy,
    double estimatedMonthlyKafkaCostUsdModern,
    double estimatedCostSavingsPercent,
    double legacyElapsedMs,
    double modernElapsedMs,
    String sampleLegacyCdcEnvelopeJson,
    String sampleModernOutboxEventJson,
    String costModelNotes
) {
}
