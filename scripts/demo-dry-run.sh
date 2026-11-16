#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PORT="${LEDGERFAN_DEMO_PORT:-18080}"
BASE_URL="http://localhost:${PORT}"
JAR_PATH="${ROOT_DIR}/target/ledgerfan-fintech-aggregator-1.0.0-SNAPSHOT.jar"
LOG_FILE="/tmp/ledgerfan-demo-${PORT}.log"
APP_PID=""

cleanup() {
  if [[ -n "${APP_PID}" ]] && kill -0 "${APP_PID}" 2>/dev/null; then
    echo ""
    echo "[cleanup] Stopping LedgerFan demo server (PID ${APP_PID})..."
    kill "${APP_PID}" 2>/dev/null || true
    wait "${APP_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

banner() {
  echo ""
  echo "================================================================================"
  echo "  $1"
  echo "================================================================================"
}

if lsof -iTCP:"${PORT}" -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "ERROR: Port ${PORT} is already in use. Set LEDGERFAN_DEMO_PORT to an open port and retry." >&2
  exit 1
fi

banner "LedgerFan Fintech Aggregator & Outbox Event Mesh — Live Dry-Run Demo"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "[build] Packaging executable JAR (mvn -q -DskipTests package)..."
  mvn -q -DskipTests package
fi

echo "[boot] Starting LedgerFan on ${BASE_URL} (Java 21 Virtual Threads + --enable-preview)..."
java --enable-preview -jar "${JAR_PATH}" --server.port="${PORT}" > "${LOG_FILE}" 2>&1 &
APP_PID=$!

for i in $(seq 1 40); do
  if curl -sf "${BASE_URL}/actuator/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${APP_PID}" 2>/dev/null; then
    echo "ERROR: Application exited unexpectedly. Logs:" >&2
    cat "${LOG_FILE}" >&2
    exit 1
  fi
  sleep 0.5
done

if ! curl -sf "${BASE_URL}/actuator/health" >/dev/null 2>&1; then
  echo "ERROR: Timed out waiting for ${BASE_URL}/actuator/health" >&2
  cat "${LOG_FILE}" >&2
  exit 1
fi

echo "[boot] Healthy: $(curl -sf "${BASE_URL}/actuator/health")"

# Warm up JIT & Resilience4j registries once
curl -sf "${BASE_URL}/api/v1/accounts/ACC-WARMUP/financial-profile?bypassCache=true" >/dev/null

# ------------------------------------------------------------------------------
# Stage 1: Cold/Live 12-Upstream Concurrent Fan-Out
# ------------------------------------------------------------------------------
banner "Stage 1: Live 12-Upstream Concurrent Fan-Out (Java 21 StructuredTaskScope)"
STAGE1_JSON="$(curl -sf "${BASE_URL}/api/v1/accounts/ACC-DEMO-777/financial-profile?bypassCache=true")"
python3 - <<'PY' "${STAGE1_JSON}"
import json, sys
d = json.loads(sys.argv[1])
m = d["metadata"]
print(f"Account ID       : {m['accountId']}")
print(f"Trace ID         : {m['traceId']}")
print(f"Status           : {m['status']} ({m['succeededCount']}/{m['totalUpstreams']} upstreams)")
print(f"Wall-Clock Time  : {m['totalElapsedMs']} ms  (SLA <= {m['slaTargetMs']} ms, met={m['slaMet']})")
print(f"Sequential Sum   : {m['sumSequentialUpstreamMs']} ms  ->  Speedup: {m['speedupFactor']}x")
print("\nSample Federated Payloads (REST + GraphQL):")
print("  [REST]    checkingAccount :", json.dumps(d["checkingAccount"]))
print("  [REST]    generalLedger   :", json.dumps(d["generalLedger"]))
print("  [GraphQL] kycCompliance   :", json.dumps(d["kycCompliance"]))
print("  [GraphQL] creditCards     :", json.dumps(d["creditCards"]))
PY

# ------------------------------------------------------------------------------
# Stage 2: OpenTelemetry Distributed Trace & ASCII Latency Waterfall
# ------------------------------------------------------------------------------
banner "Stage 2: OpenTelemetry Distributed Trace & 13-Span Virtual-Thread Waterfall"
STAGE2_JSON="$(curl -sf "${BASE_URL}/api/v1/accounts/ACC-DEMO-777/financial-profile/waterfall")"
python3 - <<'PY' "${STAGE2_JSON}"
import json, sys
d = json.loads(sys.argv[1])
print(d["asciiWaterfall"])
PY

# ------------------------------------------------------------------------------
# Stage 3: Tier-1 Redis Read-Through Cache Hit
# ------------------------------------------------------------------------------
banner "Stage 3: Tier-1 Read-Through Cache Hit + Cache Telemetry"
STAGE3_JSON="$(curl -sf "${BASE_URL}/api/v1/accounts/ACC-DEMO-777/financial-profile?bypassCache=false")"
CACHE_STATS_JSON="$(curl -sf "${BASE_URL}/api/v1/cache/stats")"
python3 - <<'PY' "${STAGE3_JSON}" "${CACHE_STATS_JSON}"
import json, sys
d = json.loads(sys.argv[1])
stats = json.loads(sys.argv[2])
m = d["metadata"]
print(f"Status           : {m['status']}")
print(f"Cache Hits       : {m['cacheHitCount']}/{m['totalUpstreams']} upstreams")
print(f"Wall-Clock Time  : {m['totalElapsedMs']} ms")
print(f"Cache Stats      : {json.dumps(stats)}")
PY

# ------------------------------------------------------------------------------
# Stage 4: Fault Injection & Graceful Partial Degradation
# ------------------------------------------------------------------------------
banner "Stage 4: Fault Injection (220ms Delay on 2 GraphQL Upstreams) & Partial Degradation"
STAGE4_CACHED="$(curl -sf "${BASE_URL}/api/v1/accounts/ACC-DEMO-777/financial-profile?bypassCache=true&simulateSlowUpstreams=loan-mortgage-graphql:220,rewards-loyalty-graphql:220")"
STAGE4_UNCACHED="$(curl -sf "${BASE_URL}/api/v1/accounts/ACC-DEMO-UNCACHED/financial-profile?bypassCache=true&simulateSlowUpstreams=loan-mortgage-graphql:220,rewards-loyalty-graphql:220")"
python3 - <<'PY' "${STAGE4_CACHED}" "${STAGE4_UNCACHED}"
import json, sys
cached = json.loads(sys.argv[1])
uncached = json.loads(sys.argv[2])

cm = cached["metadata"]
print("4A) Previously Cached Account (Rescued via Tier-2 STALE_FALLBACK):")
print(f"    Status={cm['status']}, Wall={cm['totalElapsedMs']} ms (SLA met={cm['slaMet']})")
print(f"    Live={cm['succeededCount']}, StaleFallback={cm['staleFallbackCount']}, Degraded={cm['degradedUpstreams']}")
print(f"    loan-mortgage-graphql status : {cm['upstreamStatuses']['loan-mortgage-graphql']} (payload preserved={cached['loansAndMortgages'] is not None})")
print(f"    rewards-loyalty-graphql status: {cm['upstreamStatuses']['rewards-loyalty-graphql']} (payload preserved={cached['rewardsAndLoyalty'] is not None})")

um = uncached["metadata"]
print("\n4B) Uncached Account (Isolated TIMEOUT without blocking 10 healthy upstreams):")
print(f"    Status={um['status']}, Wall={um['totalElapsedMs']} ms (SLA met={um['slaMet']})")
print(f"    Live={um['succeededCount']}, Degraded={um['degradedCount']} {um['degradedUpstreams']}")
print(f"    loan-mortgage-graphql status : {um['upstreamStatuses']['loan-mortgage-graphql']} (payload={uncached['loansAndMortgages']})")
print(f"    checking-accounts-rest status: {um['upstreamStatuses']['checking-accounts-rest']} (balance={uncached['checkingAccount']['availableBalance']} {uncached['checkingAccount']['currency']})")
PY

# ------------------------------------------------------------------------------
# Stage 5: Zero-Dual-Write Transactional Outbox & Redis Resource Cache
# ------------------------------------------------------------------------------
banner "Stage 5: Zero-Dual-Write Transactional Outbox & Redis Resource Cache Hydration"
STAGE5_POST="$(curl -sf -X POST "${BASE_URL}/api/v1/ledger/transactions" \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "ACC-DEMO-777",
    "counterpartyAccountId": "CPTY-PAYROLL-01",
    "amount": 1250.00,
    "currency": "GBP",
    "transactionType": "CREDIT",
    "reference": "DEMO-DRY-RUN-PAYROLL"
  }')"
STAGE5_EVENTS="$(curl -sf "${BASE_URL}/api/v1/outbox/events?publishedLimit=3")"
python3 - <<'PY' "${STAGE5_POST}" "${STAGE5_EVENTS}"
import json, sys
post = json.loads(sys.argv[1])
events = json.loads(sys.argv[2])
print("Transaction Commit & Outbox Relay:")
print(" ", json.dumps(post, indent=2))
print("\nEvent Mesh Delivery Status:")
print(f"  pendingOutboxRows={events['pendingOutboxRows']}, publishedOutboxRows={events['publishedOutboxRows']}, meshTotalPublishedMessages={events['meshTotalPublishedMessages']}")
if events["recentMeshMessages"]:
    msg = events["recentMeshMessages"][0]
    print(f"  Latest Kafka Topic Message -> topic={msg['topic']}, key={msg['partitionKey']}, bytes={msg['payloadBytes']}, headers={json.dumps(msg['headers'])}")
PY

# ------------------------------------------------------------------------------
# Stage 6: Legacy Table CDC vs. Transactional Outbox Benchmark (500 Transfers)
# ------------------------------------------------------------------------------
banner "Stage 6: Legacy Table-Level CDC vs. Modern Outbox Benchmark (500 Transfers)"
STAGE6_BENCH="$(curl -sf -X POST "${BASE_URL}/api/v1/benchmarks/cdc-vs-outbox?transactions=500")"
python3 - <<'PY' "${STAGE6_BENCH}"
import json, sys
b = json.loads(sys.argv[1])
print(f"Transactions Processed          : {b['transactionsProcessed']}")
print(f"Events Emitted (CDC -> Outbox)  : {b['legacyCdcTotalEvents']} -> {b['modernOutboxTotalEvents']} (-{b['eventCountReductionPercent']}%)")
print(f"Avg Event Size (CDC -> Outbox)  : {b['legacyCdcAvgEventBytes']} B -> {b['modernOutboxAvgEventBytes']} B")
print(f"Wire Volume (CDC -> Outbox)     : {b['legacyCdcTotalBytes']:,} B -> {b['modernOutboxTotalBytes']:,} B (-{b['payloadVolumeReductionPercent']}%)")
print(f"Kafka Partitions (RF=3)         : {b['legacyPartitionsRequired']} -> {b['modernPartitionsRequired']} (-{b['partitionReductionPercent']}%)")
print(f"Projected Monthly Kafka Cost    : ${b['estimatedMonthlyKafkaCostUsdLegacy']:,.2f} -> ${b['estimatedMonthlyKafkaCostUsdModern']:,.2f} (-{b['estimatedCostSavingsPercent']}%)")
print(f"\nModel Notes: {b['costModelNotes']}")
PY

banner "Dry-Run Demo Completed Successfully (All 6 Stages Verified)"
