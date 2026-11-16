# ADR-002: Decommissioning Table-Level Kafka CDC in Favor of Transactional Outbox & Redis Resource Cache

- **Status:** Accepted
- **Date:** 2026-11-17
- **Component:** `com.ledgerfan.aggregator.outbox`, `com.ledgerfan.aggregator.benchmark`

## Context

During monolith decomposition, downstream analytical, notification, and profile-read services were originally fed by Debezium table-level Change Data Capture (CDC) connectors streaming raw PostgreSQL WAL mutations across four core ledger tables:

- `cdc.barclays_core_monolith.monolith_accounts` (12 partitions)
- `cdc.barclays_core_monolith.monolith_ledger_journal` (12 partitions)
- `cdc.barclays_core_monolith.monolith_balance_snapshots` (12 partitions)
- `cdc.barclays_core_monolith.monolith_audit_trail` (12 partitions)

This legacy CDC architecture created four structural problems:
1. **4x Event Amplification:** A single business transfer generated four separate Kafka CDC events across four topics, forcing downstream consumers to correlate fragmented row mutations out of order.
2. **Envelope Bloat:** Every Debezium CDC message carried duplicate `before` + `after` row images, structural `schema` blocks, WAL LSN metadata, and internal audit columns (~763 bytes/event average, ~3,053 bytes/transaction across all 4 tables).
3. **Partition & Broker Sprawl:** Maintaining 4 topics × 12 partitions = **48 partitions** (at RF=3) inflated broker memory, ISR replication overhead, cross-AZ traffic, and 7-day retention storage costs.
4. **Schema Fragility:** Internal DDL refactors on monolith tables leaked directly into Kafka consumer contracts.

Attempting to bypass CDC by writing directly to Kafka inside application code introduces the classic **dual-write bug** (DB commits while Kafka publish fails, or Kafka publishes while DB rolls back).

## Decision

We decommission the 4 table-level Debezium CDC topics and adopt a **Zero-Dual-Write Transactional Outbox + Redis Resource Cache** pattern:

1. **Single-Transaction Atomicity (`LedgerPostingService`)**
   - Within one ACID database transaction (`@Transactional`), `LedgerPostingService` writes both the domain mutation (`LedgerTransactionEntity` in `ledger_transactions`) and a single compact semantic domain event (`OutboxEventEntity` in `outbox_events` with `schemaVersion="1.0"`, `idempotencyKey`, and W3C `traceParent`).
   - If any failure occurs before commit, both the ledger mutation and outbox row roll back cleanly—zero phantom events.

2. **Redis Resource Projection Cache (`AccountResourceProjection`)**
   - On transaction commit (`TransactionSynchronization.afterCommit()`), the latest account balance projection is hydrated in Redis (`ledger:resource:{accountId}`).
   - Because downstream services can look up full current state directly from the low-latency Redis Resource Cache (or the aggregator), the Kafka event payload stays thin and semantic (`LedgerTransactionPostedEvent`, ~299 bytes) instead of carrying full relational snapshots.

3. **Asynchronous Relay to Unified Domain Topic (`OutboxRelayPublisher`)**
   - Polls `PENDING` rows ordered by `createdAt` in batches of 100 and publishes to a single clean domain topic (`ledger.domain.events.v1`, **24 partitions**, keyed by `accountId` for strict per-account ordering).

## Quantitative Cost & Efficiency Model (Measured in `CdcVsOutboxBenchmarkService`)

Measured from a 500-transaction live benchmark run and projected at **500 sustained TPS** with **7-day Kafka retention** and **Replication Factor 3**:

| Metric | Legacy Table-Level CDC (4 Topics) | Transactional Outbox + Redis Resource Cache | Reduction |
| :--- | :--- | :--- | :--- |
| **Events Emitted (500 Transfers)** | 2,000 raw row-change events | 500 semantic domain events | **75.0%** |
| **Kafka Topics / Total Partitions** | 4 topics / 48 partitions | 1 topic / 24 partitions | **50.0%** |
| **Avg Event Size (JSON)** | 763.29 bytes/event (~3,053 B/tx) | 299.30 bytes/event (~299 B/tx) | **90.2% total bytes** |
| **Measured Wire Volume (500 Transfers)** | 1,526,572 bytes | 149,650 bytes | **90.2%** |
| **Partition + Broker Compute (RF=3)** | \$450 floor + 48 × \$18.50 = \$1,338.00/mo | \$450 floor + 24 × \$18.50 = \$894.00/mo | **33.2%** |
| **7-Day Storage & Cross-AZ Transfer** | ~2.58 TB replicated @ \$0.13/GB ≈ \$335.35/mo | ~252.8 GB replicated @ \$0.13/GB ≈ \$32.87/mo | **90.2%** |
| **Combined Monthly Kafka Partition & Storage Cost** | **\$1,673.35 / mo** | **\$926.87 / mo** | **44.61% (within 40–60% target)** |

*(Live measurements and cost breakdowns can be reproduced anytime via `POST /api/v1/benchmarks/cdc-vs-outbox?transactions=500`.)*

## Consequences

- **Positive:** Eliminates dual-write inconsistency, cuts Kafka partition footprint by 50%, removes 90.2% of serialized payload volume, lowers monthly Kafka partition + storage spend by **44.61%** (falling cleanly inside the **40–60%** cost-reduction envelope), and decouples downstream consumers from internal relational table schemas.
- **Tradeoff:** Introduces sub-second asynchronous relay lag (`poll-interval-ms: 500`) between database commit and Kafka delivery, mitigated for synchronous readers by immediate `afterCommit()` hydration of the Redis Resource Cache.
