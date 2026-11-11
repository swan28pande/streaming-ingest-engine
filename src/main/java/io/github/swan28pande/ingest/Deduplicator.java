package io.github.swan28pande.ingest;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * In-memory, TTL + size bounded set of recently seen record keys.
 *
 * <p>Keys are reserved before publishing and released if the publish fails, so a client retry
 * after a Kafka error isn't mistaken for a duplicate. Being per-process, it catches retries
 * and replays within one instance; run one instance per partition of the key space, or swap in
 * a shared store (e.g. Redis), for cluster-wide dedup.
 */
final class Deduplicator {
    private final long ttlNanos;
    private final int maxEntries;
    private final LinkedHashMap<String, Long> seen = new LinkedHashMap<>(1024, 0.75f, false);

    Deduplicator(long ttlSeconds, int maxEntries) {
        this.ttlNanos = ttlSeconds * 1_000_000_000L;
        this.maxEntries = maxEntries;
    }

    /** True if {@code key} was not seen within the TTL; it is then reserved. */
    synchronized boolean tryReserve(String key) {
        long now = System.nanoTime();
        evict(now);
        Long at = seen.get(key);
        if (at != null && now - at < ttlNanos) return false;
        seen.remove(key);        // re-insert at the tail so insertion order stays time order
        seen.put(key, now);
        return true;
    }

    synchronized void release(String key) {
        seen.remove(key);
    }

    synchronized int size() {
        return seen.size();
    }

    private void evict(long now) {
        var it = seen.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> e = it.next();
            if (seen.size() > maxEntries || now - e.getValue() >= ttlNanos) it.remove();
            else break; // insertion order == time order, so the rest are newer
        }
    }
}
