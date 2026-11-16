package com.ledgerfan.aggregator.cache;

/**
 * Point-in-time counters for the read-through + stale-fallback cache.
 */
public record CacheStatsSnapshot(
    long freshHits,
    long misses,
    long staleFallbacks,
    long writes,
    long evictions,
    int l1EntryCount,
    boolean redisEnabled
) {
}
