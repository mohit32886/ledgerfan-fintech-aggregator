package com.ledgerfan.aggregator.api;

import com.ledgerfan.aggregator.benchmark.BenchmarkComparisonReport;
import com.ledgerfan.aggregator.benchmark.CdcVsOutboxBenchmarkService;
import com.ledgerfan.aggregator.outbox.AccountResourceProjection;
import com.ledgerfan.aggregator.outbox.EventMeshTransport;
import com.ledgerfan.aggregator.outbox.LedgerPostingResult;
import com.ledgerfan.aggregator.outbox.LedgerPostingService;
import com.ledgerfan.aggregator.outbox.LedgerTransferRequest;
import com.ledgerfan.aggregator.outbox.OutboxEventEntity;
import com.ledgerfan.aggregator.outbox.OutboxEventRepository;
import com.ledgerfan.aggregator.outbox.OutboxEventStatus;
import com.ledgerfan.aggregator.outbox.OutboxRelayPublisher;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST API for zero-dual-write ledger postings, outbox event mesh inspection,
 * and the Legacy CDC vs. Modern Transactional Outbox benchmark.
 */
@RestController
@RequestMapping("/api/v1")
public class OutboxBenchmarkController {

    private final LedgerPostingService postingService;
    private final OutboxRelayPublisher relayPublisher;
    private final OutboxEventRepository outboxEventRepository;
    private final EventMeshTransport eventMeshTransport;
    private final CdcVsOutboxBenchmarkService benchmarkService;

    public OutboxBenchmarkController(
        LedgerPostingService postingService,
        OutboxRelayPublisher relayPublisher,
        OutboxEventRepository outboxEventRepository,
        EventMeshTransport eventMeshTransport,
        CdcVsOutboxBenchmarkService benchmarkService
    ) {
        this.postingService = postingService;
        this.relayPublisher = relayPublisher;
        this.outboxEventRepository = outboxEventRepository;
        this.eventMeshTransport = eventMeshTransport;
        this.benchmarkService = benchmarkService;
    }

    @PostMapping("/ledger/transactions")
    public LedgerTransactionResponse postTransaction(@Valid @RequestBody LedgerTransferRequest request) {
        LedgerPostingResult posting = postingService.postTransaction(request);
        OutboxRelayPublisher.OutboxRelayFlushResult relayFlush = relayPublisher.flushPendingEvents();
        AccountResourceProjection projection = postingService.getResourceProjection(request.accountId()).orElse(null);
        return new LedgerTransactionResponse(posting, relayFlush, projection);
    }

    @GetMapping("/outbox/events")
    public OutboxStatusResponse listOutboxEvents(@RequestParam(defaultValue = "20") int publishedLimit) {
        List<OutboxEventEntity> recentEvents = outboxEventRepository.findTop50ByOrderByCreatedAtDesc();
        long pendingCount = outboxEventRepository.countByStatus(OutboxEventStatus.PENDING);
        long publishedCount = outboxEventRepository.countByStatus(OutboxEventStatus.PUBLISHED);
        return new OutboxStatusResponse(
            pendingCount,
            publishedCount,
            eventMeshTransport.totalPublishedCount(),
            eventMeshTransport.totalPublishedBytes(),
            recentEvents,
            eventMeshTransport.recentMessages(publishedLimit)
        );
    }

    @PostMapping("/benchmarks/cdc-vs-outbox")
    public BenchmarkComparisonReport runCdcVsOutboxBenchmark(
        @RequestParam(defaultValue = "500") int transactions
    ) {
        return benchmarkService.runComparison(transactions);
    }

    public record LedgerTransactionResponse(
        LedgerPostingResult posting,
        OutboxRelayPublisher.OutboxRelayFlushResult relayFlush,
        AccountResourceProjection redisResourceProjection
    ) {
    }

    public record OutboxStatusResponse(
        long pendingOutboxRows,
        long publishedOutboxRows,
        long meshTotalPublishedMessages,
        long meshTotalPublishedBytes,
        List<OutboxEventEntity> recentOutboxRows,
        List<EventMeshTransport.PublishedTopicMessage> recentMeshMessages
    ) {
    }
}
