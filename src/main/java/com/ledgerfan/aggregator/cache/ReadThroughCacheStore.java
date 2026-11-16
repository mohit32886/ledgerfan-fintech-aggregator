package com.ledgerfan.aggregator.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Read-through and stale-while-degraded cache store backed by an ultra-fast L1 map
 * with optional L2 Redis synchronization for distributed aggregator pods and resource projections.
 */
@Component
public class ReadThroughCacheStore {

    private static final Logger log = LoggerFactory.getLogger(ReadThroughCacheStore.class);

    private record RedisEnvelope(
        String payloadJson,
        Instant cachedAt,
        Instant freshUntil,
        Instant staleUntil
    ) {
    }

    private final Map<String, CachedValue<Object>> localStore = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    private final boolean redisEnabled;

    private final LongAdder freshHits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder staleFallbacks = new LongAdder();
    private final LongAdder writes = new LongAdder();
    private final LongAdder evictions = new LongAdder();

    public ReadThroughCacheStore(
        ObjectMapper objectMapper,
        ObjectProvider<StringRedisTemplate> redisTemplateProvider,
        @Value("${ledgerfan.cache.redis-enabled:false}") boolean redisEnabled
    ) {
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplateProvider.getIfAvailable();
        this.redisEnabled = redisEnabled && this.redisTemplate != null;
    }

    public <T> Optional<T> getFresh(String namespace, String key, Class<T> type) {
        String compositeKey = compositeKey(namespace, key);
        Instant now = Instant.now();

        CachedValue<T> entry = lookupEntry(compositeKey, type);
        if (entry != null && entry.isFresh(now)) {
            freshHits.increment();
            return Optional.ofNullable(entry.value());
        }

        misses.increment();
        return Optional.empty();
    }

    public <T> Optional<T> getStaleFallback(String namespace, String key, Class<T> type) {
        String compositeKey = compositeKey(namespace, key);
        Instant now = Instant.now();

        CachedValue<T> entry = lookupEntry(compositeKey, type);
        if (entry != null && entry.isUsableStale(now)) {
            staleFallbacks.increment();
            return Optional.ofNullable(entry.value());
        }

        return Optional.empty();
    }

    public <T> void put(String namespace, String key, T value, Duration freshTtl, Duration staleTtl) {
        if (value == null) {
            return;
        }
        String compositeKey = compositeKey(namespace, key);
        Instant now = Instant.now();
        Instant freshUntil = now.plus(freshTtl);
        Duration effectiveStaleTtl = staleTtl.compareTo(freshTtl) >= 0 ? staleTtl : freshTtl;
        Instant staleUntil = now.plus(effectiveStaleTtl);

        localStore.put(compositeKey, new CachedValue<>(value, now, freshUntil, staleUntil));
        writes.increment();

        if (redisEnabled) {
            writeToRedis(compositeKey, value, now, freshUntil, staleUntil, effectiveStaleTtl);
        }
    }

    public void evict(String namespace, String key) {
        String compositeKey = compositeKey(namespace, key);
        localStore.remove(compositeKey);
        evictions.increment();
        if (redisEnabled) {
            try {
                redisTemplate.delete(compositeKey);
            } catch (Exception ex) {
                log.warn("Redis evict failed for key={}: {}", compositeKey, ex.getMessage());
            }
        }
    }

    /**
     * Marks the fresh window as expired while retaining the stale fallback window,
     * useful for simulating stale-while-degraded behavior in tests and drills.
     */
    public void expireFreshWindow(String namespace, String key) {
        String compositeKey = compositeKey(namespace, key);
        localStore.computeIfPresent(compositeKey, (k, existing) -> new CachedValue<>(
            existing.value(),
            existing.cachedAt(),
            Instant.now().minusSeconds(1),
            existing.staleUntil()
        ));
    }

    public void clearAll() {
        localStore.clear();
    }

    public CacheStatsSnapshot stats() {
        return new CacheStatsSnapshot(
            freshHits.sum(),
            misses.sum(),
            staleFallbacks.sum(),
            writes.sum(),
            evictions.sum(),
            localStore.size(),
            redisEnabled
        );
    }

    @SuppressWarnings("unchecked")
    private <T> CachedValue<T> lookupEntry(String compositeKey, Class<T> type) {
        CachedValue<Object> local = localStore.get(compositeKey);
        if (local != null) {
            if (!local.isUsableStale(Instant.now())) {
                localStore.remove(compositeKey);
                return null;
            }
            if (type.isInstance(local.value())) {
                return (CachedValue<T>) local;
            }
            // Convert if deserialized as generic structure
            T converted = objectMapper.convertValue(local.value(), type);
            return new CachedValue<>(converted, local.cachedAt(), local.freshUntil(), local.staleUntil());
        }

        if (redisEnabled) {
            return readFromRedis(compositeKey, type);
        }
        return null;
    }

    private <T> void writeToRedis(
        String compositeKey,
        T value,
        Instant cachedAt,
        Instant freshUntil,
        Instant staleUntil,
        Duration ttl
    ) {
        try {
            String payloadJson = objectMapper.writeValueAsString(value);
            String envelopeJson = objectMapper.writeValueAsString(
                new RedisEnvelope(payloadJson, cachedAt, freshUntil, staleUntil)
            );
            redisTemplate.opsForValue().set(compositeKey, envelopeJson, ttl);
        } catch (Exception ex) {
            log.warn("Redis write failed for key={}, falling back to L1 only: {}", compositeKey, ex.getMessage());
        }
    }

    private <T> CachedValue<T> readFromRedis(String compositeKey, Class<T> type) {
        try {
            String envelopeJson = redisTemplate.opsForValue().get(compositeKey);
            if (envelopeJson == null || envelopeJson.isBlank()) {
                return null;
            }
            RedisEnvelope envelope = objectMapper.readValue(envelopeJson, RedisEnvelope.class);
            T payload = objectMapper.readValue(envelope.payloadJson(), type);
            CachedValue<T> cached = new CachedValue<>(
                payload, envelope.cachedAt(), envelope.freshUntil(), envelope.staleUntil()
            );
            if (cached.isUsableStale(Instant.now())) {
                localStore.put(compositeKey, new CachedValue<>(
                    payload, envelope.cachedAt(), envelope.freshUntil(), envelope.staleUntil()
                ));
                return cached;
            }
            return null;
        } catch (JsonProcessingException | RuntimeException ex) {
            log.warn("Redis read failed for key={}: {}", compositeKey, ex.getMessage());
            return null;
        }
    }

    private static String compositeKey(String namespace, String key) {
        return "ledgerfan:" + namespace + ":" + key;
    }
}
