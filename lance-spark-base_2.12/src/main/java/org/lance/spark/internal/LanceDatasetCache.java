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
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.Utils;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-wide cache of opened Lance {@link Dataset} instances, keyed on {@code (datasetUri,
 * version, storageOptionsHash)}.
 *
 * <p><strong>Why this exists:</strong> Spark batch reads create one {@link LanceFragmentScanner}
 * per fragment. Without this cache, each fragment calls {@code
 * Utils.openDatasetBuilder(...).build()} — costing ~5 ms of native Dataset construction per
 * fragment even after PR#1 (process-wide {@code ObjectStoreRegistry} reuse). For a 234-fragment
 * store_sales scan that accumulates ~1.2 s of fixed cost (≈ 150 ms wall on 8-core local). The cache
 * collapses that to a single open per JVM lifetime per dataset identity.
 *
 * <p><strong>Single-flight:</strong> {@link ConcurrentHashMap#computeIfAbsent} guarantees only one
 * thread per key opens the dataset cold; other threads arriving concurrently for the same key block
 * on that one open and reuse the result. Different keys open in parallel.
 *
 * <p><strong>Lifecycle:</strong> Cache holds strong references for the JVM lifetime — there is no
 * automatic eviction. The default cap of 16 distinct datasets covers typical OLAP workloads (TPC-DS
 * uses ≤24 tables, but a single Spark session usually only touches a handful per query). Callers
 * must <em>not</em> {@link Dataset#close()} a cached dataset; doing so frees the native handle and
 * corrupts subsequent cache hits. {@link LanceFragmentScanner} is responsible for skipping {@code
 * dataset.close()} when the dataset came from this cache.
 *
 * <p><strong>Toggles:</strong>
 *
 * <ul>
 *   <li>{@code -Dlance.spark.dataset_cache_enabled=false} — bypass cache, restore per-fragment open
 *       behavior (for A/B verification).
 *   <li>{@code -Dlance.spark.dataset_cache_max=N} — override the cap; once reached, new keys still
 *       open directly but are not cached (no eviction to avoid races with in-flight readers).
 * </ul>
 */
public final class LanceDatasetCache {

  static final String CACHE_ENABLED_PROPERTY = "lance.spark.dataset_cache_enabled";
  static final String CACHE_MAX_SIZE_PROPERTY = "lance.spark.dataset_cache_max";
  static final String DEFAULT_MAX_SIZE = "16";

  private static final ConcurrentMap<Key, Dataset> CACHE = new ConcurrentHashMap<>();

  private LanceDatasetCache() {}

  /**
   * Read fresh on each call so {@link System#setProperty(String, String)} in tests takes effect
   * without process restart. The lookup cost is a single {@link System#getProperty(String, String)}
   * (~100 ns) — negligible compared to the dataset open cost the cache exists to amortize.
   */
  private static boolean isEnabled() {
    return Boolean.parseBoolean(System.getProperty(CACHE_ENABLED_PROPERTY, "true"));
  }

  private static int maxSize() {
    return Integer.parseInt(System.getProperty(CACHE_MAX_SIZE_PROPERTY, DEFAULT_MAX_SIZE));
  }

  /**
   * Return a cached or freshly-opened {@link Dataset} for this partition's dataset identity. Caller
   * <em>must not</em> call {@link Dataset#close()} on the returned instance when {@link
   * OpenResult#cached()} would return true (use {@link LanceFragmentScanner#close()} which handles
   * this).
   */
  public static OpenResult getOrOpen(LanceInputPartition inputPartition) {
    if (!isEnabled()) {
      Dataset ds = openDirect(inputPartition);
      return new OpenResult(ds, false);
    }
    Key key = Key.from(inputPartition);
    if (CACHE.size() >= maxSize() && !CACHE.containsKey(key)) {
      // Cap reached for a key not already cached — open directly to avoid
      // unbounded growth. The caller closes the dataset normally.
      Dataset ds = openDirect(inputPartition);
      return new OpenResult(ds, false);
    }
    Dataset ds = CACHE.computeIfAbsent(key, k -> openDirect(inputPartition));
    return new OpenResult(ds, true);
  }

  /**
   * Test/diagnostics hook: drop all cached entries and close their datasets. Not safe to call while
   * readers are active.
   */
  public static void clear() {
    CACHE
        .values()
        .forEach(
            ds -> {
              try {
                ds.close();
              } catch (Exception ignored) {
                // best-effort
              }
            });
    CACHE.clear();
  }

  /** Number of currently cached datasets; mainly for diagnostics/tests. */
  public static int size() {
    return CACHE.size();
  }

  private static Dataset openDirect(LanceInputPartition inputPartition) {
    return Utils.openDatasetBuilder(inputPartition.getReadOptions())
        .initialStorageOptions(inputPartition.getInitialStorageOptions())
        .build();
  }

  /** Carries both the {@link Dataset} and a flag for whether the caller owns the close. */
  public static final class OpenResult {
    private final Dataset dataset;
    private final boolean cached;

    OpenResult(Dataset dataset, boolean cached) {
      this.dataset = dataset;
      this.cached = cached;
    }

    public Dataset dataset() {
      return dataset;
    }

    /** True when {@link Dataset#close()} must NOT be called by the caller. */
    public boolean cached() {
      return cached;
    }
  }

  /**
   * Cache key — captures the inputs that determine dataset identity. Visible for testing the
   * equals/hashCode contract without requiring a Dataset.
   */
  static final class Key {
    final String datasetUri;
    final Integer version;
    final int storageOptionsHash;

    Key(String datasetUri, Integer version, int storageOptionsHash) {
      this.datasetUri = datasetUri;
      this.version = version;
      this.storageOptionsHash = storageOptionsHash;
    }

    static Key from(LanceInputPartition inputPartition) {
      String uri = inputPartition.getReadOptions().getDatasetUri();
      Integer version = inputPartition.getReadOptions().getVersion();
      // Hash the storage options as a stable, ordered sequence so cache keys
      // are deterministic and not sensitive to map iteration order.
      Map<String, String> opts = inputPartition.getInitialStorageOptions();
      int hash = 0;
      if (opts != null && !opts.isEmpty()) {
        TreeMap<String, String> ordered = new TreeMap<>(opts);
        hash = ordered.hashCode();
      }
      return new Key(uri, version, hash);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Key)) return false;
      Key other = (Key) o;
      return storageOptionsHash == other.storageOptionsHash
          && Objects.equals(datasetUri, other.datasetUri)
          && Objects.equals(version, other.version);
    }

    @Override
    public int hashCode() {
      return Objects.hash(datasetUri, version, storageOptionsHash);
    }
  }
}
