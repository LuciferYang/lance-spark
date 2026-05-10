/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.internal;

import org.lance.Dataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * JVM-local, reference-counted, TTL-evicted cache for Lance {@link Dataset} handles, shared across
 * all fragment scans in a single executor/driver JVM.
 *
 * <p>Opening a Lance dataset is expensive on cloud object storage: it issues a LIST, a HEAD on each
 * manifest candidate, and a GET on the chosen manifest. When a Spark job plans a scan over N
 * fragments, a naïve implementation re-opens the dataset N times per task, multiplying that cost.
 * Because Spark pins the dataset version on the driver before dispatching the scan, the same URI +
 * version + storage-options combination is opened many times in one JVM — ideal for a cache.
 *
 * <p>Lifecycle is reference-counted:
 *
 * <ul>
 *   <li>{@link #checkout(CacheKey, Supplier)} returns a cached {@link Dataset} (loading via the
 *       supplied {@code loader} on miss) and increments the entry's ref-count.
 *   <li>{@link #release(CacheKey)} decrements the ref-count; on transition to zero, the entry
 *       becomes eligible for eviction after {@value #ENV_TTL_SECONDS} seconds (default 600).
 *   <li>Eviction runs passively on each MISS via {@link #sweepExpired()}: a CAS flips the ref-count
 *       from 0 to {@link Integer#MIN_VALUE}, the map entry is removed, and {@code Dataset.close()}
 *       runs. Checkouts that race with the sweep detect the negative ref-count and retry; the
 *       loader then repopulates a fresh entry.
 * </ul>
 *
 * <p>Cache keys are opaque values computed by the caller (see {@link CacheKey}). The caller is
 * responsible for making a key stable across fragment scans that should share a Dataset — usually
 * by including the driver-pinned version, URI, catalog name, and a hash of the merged storage
 * options.
 *
 * <p>Configuration:
 *
 * <ul>
 *   <li>Enabled per-query via the {@code dataset_cache_enabled} read option (default {@code true}).
 *   <li>TTL configurable via the {@value #ENV_TTL_SECONDS} env var or Java system property, in
 *       seconds. Default {@value #DEFAULT_TTL_SECONDS}.
 * </ul>
 *
 * <p>Metrics ({@link Stats}) are exposed via {@link #snapshot()} for harness tests and ad-hoc
 * observability; {@link #clear()} is intended for test teardown and closes all held datasets.
 */
public final class LanceDatasetCache {

  private static final Logger LOG = LoggerFactory.getLogger(LanceDatasetCache.class);

  /** Env var / system property controlling how long an idle entry lives before eviction. */
  public static final String ENV_TTL_SECONDS = "LANCE_DATASET_CACHE_TTL_SECONDS";

  /** Default TTL in seconds when {@link #ENV_TTL_SECONDS} is unset or invalid. */
  public static final long DEFAULT_TTL_SECONDS = 600L;

  private static volatile LanceDatasetCache instance;

  // ---------- Static API ----------

  /**
   * Returns a dataset for the given key, loading it via {@code loader} on cache miss. Increments
   * the entry's ref-count; caller MUST invoke {@link #release(CacheKey)} with the same key when
   * done.
   */
  public static Dataset checkout(CacheKey key, Supplier<Dataset> loader) {
    return getInstance().checkoutInternal(key, loader);
  }

  /** Decrements the entry's ref-count. Does not close the dataset; TTL sweep handles eviction. */
  public static void release(CacheKey key) {
    getInstance().releaseInternal(key);
  }

  /** Returns a point-in-time snapshot of cache counters. */
  public static Stats snapshot() {
    return getInstance().snapshotInternal();
  }

  /**
   * Closes all cached datasets and resets counters. Intended for test teardown; do not call while
   * scanners may still hold references.
   */
  public static void clear() {
    getInstance().clearInternal();
  }

  /** Returns the current number of entries in the cache. */
  public static int size() {
    return getInstance().map.size();
  }

  private static LanceDatasetCache getInstance() {
    LanceDatasetCache local = instance;
    if (local == null) {
      synchronized (LanceDatasetCache.class) {
        local = instance;
        if (local == null) {
          local = new LanceDatasetCache(System::nanoTime, resolveTtlNanos());
          instance = local;
        }
      }
    }
    return local;
  }

  static void resetForTesting(LanceDatasetCache replacement) {
    synchronized (LanceDatasetCache.class) {
      if (instance != null) {
        instance.clearInternal();
      }
      instance = replacement;
    }
  }

  private static long resolveTtlNanos() {
    String raw = System.getProperty(ENV_TTL_SECONDS);
    if (raw == null) {
      raw = System.getenv(ENV_TTL_SECONDS);
    }
    long seconds = DEFAULT_TTL_SECONDS;
    if (raw != null && !raw.isEmpty()) {
      try {
        seconds = Math.max(0L, Long.parseLong(raw.trim()));
      } catch (NumberFormatException ignore) {
        // keep default
      }
    }
    return TimeUnit.SECONDS.toNanos(seconds);
  }

  // ---------- Instance state (package-private for tests) ----------

  private final LongSupplier clock;
  private final long ttlNanos;
  private final ConcurrentHashMap<CacheKey, Entry> map = new ConcurrentHashMap<>();
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();

  LanceDatasetCache(LongSupplier clock, long ttlNanos) {
    this.clock = clock;
    this.ttlNanos = ttlNanos;
  }

  long ttlNanos() {
    return ttlNanos;
  }

  Dataset checkoutInternal(CacheKey key, Supplier<Dataset> loader) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(loader, "loader");
    while (true) {
      boolean[] wasLoaded = new boolean[1];
      Entry entry =
          map.computeIfAbsent(
              key,
              k -> {
                Entry fresh = new Entry(loader.get());
                fresh.refCount.set(1);
                wasLoaded[0] = true;
                return fresh;
              });
      if (wasLoaded[0]) {
        long total = misses.incrementAndGet();
        LOG.debug("LanceDatasetCache MISS: key={} misses={} size={}", key, total, map.size());
        sweepExpired();
        return entry.dataset;
      }
      int newRef = entry.refCount.incrementAndGet();
      if (newRef < 0) {
        // Sweeper claimed this entry via CAS(0, MIN_VALUE). It will remove the map entry and
        // close the dataset. Yield briefly and retry — the next computeIfAbsent will either
        // see the eviction and load fresh, or win a race against a parallel checkout that
        // already repopulated the slot.
        Thread.yield();
        continue;
      }
      long total = hits.incrementAndGet();
      LOG.debug("LanceDatasetCache HIT: key={} hits={} refCount={}", key, total, newRef);
      return entry.dataset;
    }
  }

  void releaseInternal(CacheKey key) {
    Objects.requireNonNull(key, "key");
    Entry entry = map.get(key);
    if (entry == null) {
      // Entry was evicted before release — double release is tolerated (benign at shutdown).
      return;
    }
    // Stamp BEFORE decrementAndGet to close the TOCTOU window with sweepExpired(): if we stamped
    // only after the decrement, a concurrent sweeper could observe refCount==0 with a stale
    // lastReleasedNanos (left over from a previous release cycle whose entry has since been
    // re-used and re-released), pass the TTL check, and CAS-evict a still-hot entry.
    //
    // When refCount==1 we know this release is about to go to 0, so stamping now is safe and
    // races cannot produce a false early eviction: any concurrent checkout that incremented
    // past us would prevent the decrement from reaching 0 anyway, and the sweeper only acts on
    // refCount==0.
    if (entry.refCount.get() == 1) {
      entry.lastReleasedNanos = clock.getAsLong();
    }
    int newRef = entry.refCount.decrementAndGet();
    if (newRef == 0) {
      // Defensive re-stamp: covers the case where refCount was > 1 at the pre-stamp read (so we
      // skipped above) but concurrent releases brought it down to 0 via this one. Cheap write,
      // single-cache-line.
      entry.lastReleasedNanos = clock.getAsLong();
    }
    // newRef may be negative if the sweeper raced us; let eviction proceed.
  }

  Stats snapshotInternal() {
    return new Stats(hits.get(), misses.get(), evictions.get(), map.size());
  }

  void clearInternal() {
    for (Map.Entry<CacheKey, Entry> e : map.entrySet()) {
      Entry entry = e.getValue();
      entry.refCount.set(Integer.MIN_VALUE);
      try {
        entry.dataset.close();
      } catch (Throwable t) {
        // SLF4J treats a trailing Throwable positional arg specially and walks its cause chain.
        // Upstream Lance / object-store close errors may embed the raw URI in the cause message,
        // so emit only the class name as a `{}` substitution — do not pass the throwable itself.
        LOG.warn(
            "LanceDatasetCache.clear: error closing dataset for key={} underlying={}",
            e.getKey(),
            t.getClass().getName());
      }
    }
    map.clear();
    hits.set(0);
    misses.set(0);
    evictions.set(0);
  }

  void sweepExpired() {
    long now = clock.getAsLong();
    for (Map.Entry<CacheKey, Entry> e : map.entrySet()) {
      Entry entry = e.getValue();
      if (entry.refCount.get() != 0) {
        continue;
      }
      if (now - entry.lastReleasedNanos < ttlNanos) {
        continue;
      }
      if (!entry.refCount.compareAndSet(0, Integer.MIN_VALUE)) {
        // Another thread grabbed a ref just before we CAS'd; leave it.
        continue;
      }
      map.remove(e.getKey(), entry);
      try {
        entry.dataset.close();
      } catch (Throwable t) {
        // See note in clearInternal() — avoid passing the Throwable as the trailing SLF4J arg.
        LOG.warn(
            "LanceDatasetCache.sweep: error closing evicted dataset for key={} underlying={}",
            e.getKey(),
            t.getClass().getName());
      }
      long count = evictions.incrementAndGet();
      LOG.debug(
          "LanceDatasetCache EVICT: key={} evictions={} size={}", e.getKey(), count, map.size());
    }
  }

  // ---------- Value types ----------

  /**
   * Opaque cache key identifying a logical Dataset across fragment scans. Equality is
   * struct-equality on {@code uri}, {@code version}, {@code catalogName}, and the full {@code
   * storageOptions} map — not just a hash of the map. Using the full map eliminates the
   * (low-probability but catastrophic) cross-tenant hash-collision risk in which two queries with
   * different credentials would share the same Dataset handle on a shared JVM.
   *
   * <p>The {@code storageOptions} snapshot is stored as an unmodifiable sorted map so iteration,
   * {@code hashCode()}, and {@code toString()} are deterministic regardless of input map type.
   */
  public static final class CacheKey {
    private final String uri;
    private final int version;
    private final String catalogName;
    private final Map<String, String> storageOptions;

    /**
     * Full constructor. The passed {@code storageOptions} map is copied into an unmodifiable sorted
     * snapshot — callers may safely reuse or mutate their map afterwards. A {@code null} map is
     * treated as empty.
     */
    public CacheKey(
        String uri, int version, String catalogName, Map<String, String> storageOptions) {
      this.uri = Objects.requireNonNull(uri, "uri");
      this.version = version;
      this.catalogName = catalogName;
      this.storageOptions =
          storageOptions == null || storageOptions.isEmpty()
              ? Collections.emptyMap()
              : Collections.unmodifiableSortedMap(new TreeMap<>(storageOptions));
    }

    /**
     * Legacy constructor kept for unit-test compatibility with earlier versions that keyed on an
     * {@code int} hash of the storage-options map. The provided hash is ignored; the key is
     * constructed with an empty storage-options set. Production code MUST use the {@code Map}
     * overload so that cross-tenant hash collisions cannot swap Datasets.
     *
     * @deprecated use {@link #CacheKey(String, int, String, Map)} instead.
     */
    @Deprecated
    public CacheKey(String uri, int version, String catalogName, int storageOptsHashIgnored) {
      this(uri, version, catalogName, Collections.emptyMap());
    }

    public String uri() {
      return uri;
    }

    public int version() {
      return version;
    }

    public String catalogName() {
      return catalogName;
    }

    /** Returns the unmodifiable sorted storage-options snapshot this key was built with. */
    public Map<String, String> storageOptions() {
      return storageOptions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheKey)) {
        return false;
      }
      CacheKey other = (CacheKey) o;
      return version == other.version
          && uri.equals(other.uri)
          && Objects.equals(catalogName, other.catalogName)
          && storageOptions.equals(other.storageOptions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(uri, version, catalogName, storageOptions);
    }

    @Override
    public String toString() {
      return "CacheKey{uri="
          + LanceExceptions.redactUri(uri)
          + ", v="
          + version
          + ", catalog="
          + catalogName
          + ", optsKeys="
          + storageOptions.keySet()
          + "}";
    }
  }

  /** Immutable snapshot of cache counters. */
  public static final class Stats {
    public final long hits;
    public final long misses;
    public final long evictions;
    public final int size;

    Stats(long hits, long misses, long evictions, int size) {
      this.hits = hits;
      this.misses = misses;
      this.evictions = evictions;
      this.size = size;
    }

    @Override
    public String toString() {
      return "LanceDatasetCache.Stats{hits="
          + hits
          + ", misses="
          + misses
          + ", evictions="
          + evictions
          + ", size="
          + size
          + "}";
    }
  }

  private static final class Entry {
    final Dataset dataset;
    final AtomicInteger refCount = new AtomicInteger(0);
    volatile long lastReleasedNanos;

    Entry(Dataset dataset) {
      this.dataset = dataset;
    }
  }
}
