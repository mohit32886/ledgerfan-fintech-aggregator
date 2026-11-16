package com.ledgerfan.aggregator.outbox;

import com.ledgerfan.aggregator.benchmark.BenchmarkComparisonReport;
import com.ledgerfan.aggregator.benchmark.CdcVsOutboxBenchmarkService;
import com.ledgerfan.aggregator.cache.ReadThroughCacheStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class OutboxAndCdcBenchmarkTest {

    @Autowired
    private LedgerPostingService postingService;

    @Autowired
    private OutboxRelayPublisher relayPublisher;

    @Autowired
    private LedgerTransactionRepository transactionRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private EventMeshTransport eventMeshTransport;

    @Autowired
    private ReadThroughCacheStore cacheStore;

    @Autowired
    private CdcVsOutboxBenchmarkService benchmarkService;

    @BeforeEach
    void cleanDatabaseAndCache() {
        outboxEventRepository.deleteAll();
        transactionRepository.deleteAll();
        cacheStore.clearAll();
    }

    @Test
    void commitsLedgerAndOutboxAtomicallyHydratesRedisAndRelaysToEventMesh() {
        String accountId = "ACC-OUTBOX-1001";
        LedgerPostingResult result = postingService.postTransaction(new LedgerTransferRequest(
            accountId,
            "CPTY-900",
            new BigDecimal("250.00"),
            "GBP",
            "CREDIT",
            "PAYROLL-2026-11"
        ));

        assertThat(result.balanceAfter()).isEqualByComparingTo("10250.0000");
        assertThat(result.outboxStatus()).isEqualTo(OutboxEventStatus.PENDING);
        assertThat(result.traceParent()).startsWith("00-");
        assertThat(transactionRepository.findById(result.transactionId())).isPresent();

        OutboxEventEntity pendingRow = outboxEventRepository.findById(result.outboxEventId()).orElseThrow();
        assertThat(pendingRow.getStatus()).isEqualTo(OutboxEventStatus.PENDING);

        AccountResourceProjection projection = postingService.getResourceProjection(accountId).orElseThrow();
        assertThat(projection.latestBalance()).isEqualByComparingTo("10250.0000");
        assertThat(projection.lastTransactionId()).isEqualTo(result.transactionId());

        OutboxRelayPublisher.OutboxRelayFlushResult flush = relayPublisher.flushPendingEvents();
        assertThat(flush.publishedCount()).isEqualTo(1);
        assertThat(flush.remainingPendingCount()).isZero();

        OutboxEventEntity publishedRow = outboxEventRepository.findById(result.outboxEventId()).orElseThrow();
        assertThat(publishedRow.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
        assertThat(publishedRow.getPublishedAt()).isNotNull();
        assertThat(eventMeshTransport.totalPublishedCount()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    void rollsBackBothOutboxAndResourceCacheOnTransactionFailure() {
        String accountId = "ACC-ROLLBACK-999";
        LedgerTransferRequest request = new LedgerTransferRequest(
            accountId,
            "CPTY-888",
            new BigDecimal("500.00"),
            "GBP",
            "DEBIT",
            "TX-FAIL-TEST"
        );

        assertThatThrownBy(() -> postingService.postTransactionWithSimulatedFailure(request))
            .isInstanceOf(IllegalStateException.class);

        assertThat(transactionRepository.count()).isZero();
        assertThat(outboxEventRepository.count()).isZero();
        assertThat(postingService.getResourceProjection(accountId)).isEmpty();
    }

    @Test
    void benchmarkProves40To60PercentKafkaCostReductionVsLegacyTableCdc() {
        BenchmarkComparisonReport report = benchmarkService.runComparison(500);

        assertThat(report.transactionsProcessed()).isEqualTo(500);
        assertThat(report.legacyCdcTotalEvents()).isEqualTo(2000L);
        assertThat(report.modernOutboxTotalEvents()).isEqualTo(500L);
        assertThat(report.eventCountReductionPercent()).isEqualTo(75.0);

        assertThat(report.legacyCdcTotalBytes()).isGreaterThan(report.modernOutboxTotalBytes());
        assertThat(report.payloadVolumeReductionPercent()).isGreaterThan(80.0);

        assertThat(report.legacyPartitionsRequired()).isEqualTo(48);
        assertThat(report.modernPartitionsRequired()).isEqualTo(24);
        assertThat(report.partitionReductionPercent()).isEqualTo(50.0);

        assertThat(report.estimatedCostSavingsPercent()).isBetween(40.0, 60.0);
        assertThat(report.estimatedMonthlyKafkaCostUsdModern())
            .isLessThan(report.estimatedMonthlyKafkaCostUsdLegacy());
    }
}
