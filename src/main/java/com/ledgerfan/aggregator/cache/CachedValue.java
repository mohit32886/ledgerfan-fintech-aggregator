package com.ledgerfan.aggregator.cache;

import java.time.Instant;

/**
 * Two-tier cache entry supporting both fresh read-through hits and stale-while-degraded fallback.
 */
public record CachedValue<T>(
    T value,
    Instant cachedAt,
    Instant freshUntil,
    Instant staleUntil
) {
    public boolean isFresh(Instant now) {
        return !now.isAfter(freshUntil);
    }

    public boolean isUsableStale(Instant now) {
        return !now.isAfter(staleUntil);
    }
}
