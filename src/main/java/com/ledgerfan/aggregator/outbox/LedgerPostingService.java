package com.ledgerfan.aggregator.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgerfan.aggregator.cache.ReadThroughCacheStore;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Zero-dual-write transactional posting service.
 *
 * <p>Persists the ledger mutation and its compact {@link OutboxEventEntity} inside a single ACID
 * database transaction, then hydrates the hot Redis resource cache upon commit.
 */
@Service
public class LedgerPostingService {

    public static final String RESOURCE_CACHE_NAMESPACE = "ledger:resource";
    private static final BigDecimal DEFAULT_OPENING_BALANCE = new BigDecimal("10000.0000");

    private final LedgerTransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ReadThroughCacheStore cacheStore;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;

    public LedgerPostingService(
        LedgerTransactionRepository transactionRepository,
        OutboxEventRepository outboxEventRepository,
        ReadThroughCacheStore cacheStore,
        ObjectMapper objectMapper,
        Tracer tracer
    ) {
        this.transactionRepository = transactionRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.cacheStore = cacheStore;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
    }

    @Transactional
    public LedgerPostingResult postTransaction(LedgerTransferRequest request) {
        return postTransactionInternal(request, false);
    }

    /**
     * Transactional posting path with optional failure injection immediately after DB writes
     * to demonstrate ACID rollback guarantees (zero orphan outbox rows or stale Redis writes).
     */
    @Transactional
    public LedgerPostingResult postTransactionWithSimulatedFailure(LedgerTransferRequest request) {
        return postTransactionInternal(request, true);
    }

    private LedgerPostingResult postTransactionInternal(LedgerTransferRequest request, boolean failBeforeCommit) {
        Span span = tracer.spanBuilder("ledger.post_transaction")
            .setAttribute("account.id", request.accountId())
            .setAttribute("transaction.type", request.transactionType())
            .startSpan();

        try (Scope ignored = span.makeCurrent()) {
            Instant now = Instant.now();
            UUID txId = UUID.randomUUID();
            UUID eventId = UUID.randomUUID();

            Optional<AccountResourceProjection> cachedProjection =
                cacheStore.getFresh(RESOURCE_CACHE_NAMESPACE, request.accountId(), AccountResourceProjection.class);

            BigDecimal previousBalance = resolveCurrentBalance(request.accountId(), cachedProjection);
            long previousCount = cachedProjection.map(AccountResourceProjection::totalPostedCount).orElse(0L);
            BigDecimal newBalance = applyAmount(previousBalance, request.amount(), request.transactionType());

            LedgerTransactionEntity txEntity = new LedgerTransactionEntity(
                txId,
                request.accountId(),
                request.counterpartyAccountId(),
                request.amount().setScale(4, RoundingMode.HALF_UP),
                request.currency(),
                request.transactionType().toUpperCase(),
                newBalance,
                request.reference(),
                now
            );
            transactionRepository.save(txEntity);

            LedgerTransactionPostedEvent domainEvent = new LedgerTransactionPostedEvent(
                eventId,
                request.accountId(),
                request.counterpartyAccountId(),
                txId,
                txEntity.getTransactionType(),
                txEntity.getAmount(),
                request.currency(),
                newBalance,
                now
            );

            String payloadJson = serializeEvent(domainEvent);
            int payloadBytes = payloadJson.getBytes(StandardCharsets.UTF_8).length;
            String traceParent = formatTraceParent(span.getSpanContext());

            OutboxEventEntity outboxEntity = new OutboxEventEntity(
                eventId,
                "LedgerAccount",
                request.accountId(),
                "LedgerTransactionPosted",
                "1.0",
                "ledger-tx:" + txId,
                traceParent,
                payloadJson,
                payloadBytes,
                OutboxEventStatus.PENDING,
                now
            );
            outboxEventRepository.save(outboxEntity);

            if (failBeforeCommit) {
                throw new IllegalStateException("Simulated failure before commit for atomicity test");
            }

            AccountResourceProjection nextProjection = new AccountResourceProjection(
                request.accountId(),
                newBalance,
                request.currency(),
                txId,
                previousCount + 1,
                now
            );
            scheduleCacheHydrationOnCommit(request.accountId(), nextProjection);

            return new LedgerPostingResult(
                txId,
                eventId,
                request.accountId(),
                newBalance,
                payloadBytes,
                traceParent,
                OutboxEventStatus.PENDING,
                now
            );
        } finally {
            span.end();
        }
    }

    public Optional<AccountResourceProjection> getResourceProjection(String accountId) {
        return cacheStore.getFresh(RESOURCE_CACHE_NAMESPACE, accountId, AccountResourceProjection.class);
    }

    private BigDecimal resolveCurrentBalance(
        String accountId,
        Optional<AccountResourceProjection> cachedProjection
    ) {
        if (cachedProjection.isPresent()) {
            return cachedProjection.get().latestBalance();
        }
        List<LedgerTransactionEntity> recent =
            transactionRepository.findTop20ByAccountIdOrderByCreatedAtDesc(accountId);
        if (!recent.isEmpty()) {
            return recent.getFirst().getBalanceAfter();
        }
        return DEFAULT_OPENING_BALANCE;
    }

    private static BigDecimal applyAmount(BigDecimal current, BigDecimal delta, String transactionType) {
        String normalized = transactionType.toUpperCase();
        if (normalized.contains("DEBIT") || normalized.contains("OUT") || normalized.contains("WITHDRAW")) {
            return current.subtract(delta).setScale(4, RoundingMode.HALF_UP);
        }
        return current.add(delta).setScale(4, RoundingMode.HALF_UP);
    }

    private void scheduleCacheHydrationOnCommit(String accountId, AccountResourceProjection projection) {
        Runnable hydrate = () -> cacheStore.put(
            RESOURCE_CACHE_NAMESPACE,
            accountId,
            projection,
            Duration.ofMinutes(15),
            Duration.ofHours(1)
        );

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    hydrate.run();
                }
            });
        } else {
            hydrate.run();
        }
    }

    private String serializeEvent(LedgerTransactionPostedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize LedgerTransactionPostedEvent", ex);
        }
    }

    private static String formatTraceParent(SpanContext ctx) {
        if (ctx != null && ctx.isValid()) {
            return String.format("00-%s-%s-%s", ctx.getTraceId(), ctx.getSpanId(), ctx.getTraceFlags().asHex());
        }
        return "00-00000000000000000000000000000000-0000000000000000-00";
    }
}
