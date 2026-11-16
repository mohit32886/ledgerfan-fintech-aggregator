package com.ledgerfan.aggregator.domain;

/**
 * Execution outcome status for a single upstream call in the fan-out aggregator.
 */
public enum UpstreamStatus {
    SUCCESS,
    CACHE_HIT,
    STALE_FALLBACK,
    TIMEOUT,
    CIRCUIT_OPEN,
    BULKHEAD_FULL,
    ERROR
}
