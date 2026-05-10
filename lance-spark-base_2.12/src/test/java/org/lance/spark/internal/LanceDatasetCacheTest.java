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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LanceDatasetCacheTest {

  private static final LanceDatasetCache.CacheKey KEY_A =
      new LanceDatasetCache.CacheKey("s3://bucket/ds-a", 1, "cat", 42);
  private static final LanceDatasetCache.CacheKey KEY_B =
      new LanceDatasetCache.CacheKey("s3://bucket/ds-b", 1, "cat", 42);

  private AtomicLong now;
  private LanceDatasetCache cache;

  @BeforeEach
  void setUp() {
    now = new AtomicLong(0L);
    cache = new LanceDatasetCache(now::get, TimeUnit.SECONDS.toNanos(60));
  }

  @Test
  @DisplayName("first checkout is a MISS, second with same key is a HIT; loader runs once")
  void checkout_hitAndMiss() {
    Dataset ds = mock(Dataset.class);
    AtomicInteger loaded = new AtomicInteger();
    Supplier<Dataset> loader =
        () -> {
          loaded.incrementAndGet();
          return ds;
        };

    Dataset first = cache.checkoutInternal(KEY_A, loader);
    Dataset second = cache.checkoutInternal(KEY_A, loader);

    assertThat(first).isSameAs(ds);
    assertThat(second).isSameAs(ds);
    assertThat(loaded.get()).isEqualTo(1);
    LanceDatasetCache.Stats s = cache.snapshotInternal();
    assertThat(s.hits).isEqualTo(1);
    assertThat(s.misses).isEqualTo(1);
    assertThat(s.size).isEqualTo(1);
  }

  @Test
  @DisplayName("release does not close; dataset remains alive while refcount is held")
  void release_doesNotCloseInUseDataset() {
    Dataset ds = mock(Dataset.class);
    cache.checkoutInternal(KEY_A, () -> ds);
    cache.checkoutInternal(KEY_A, () -> ds);

    cache.releaseInternal(KEY_A);

    verify(ds, never()).close();
    LanceDatasetCache.Stats s = cache.snapshotInternal();
    assertThat(s.size).isEqualTo(1);
    assertThat(s.evictions).isEqualTo(0);
  }

  @Test
  @DisplayName("entry evicted after TTL elapses and refcount is zero")
  void eviction_afterTtl() {
    Dataset dsA = mock(Dataset.class);
    Dataset dsB = mock(Dataset.class);
    cache.checkoutInternal(KEY_A, () -> dsA);
    cache.releaseInternal(KEY_A); // refCount -> 0, stamps lastReleasedNanos=0

    // Advance time past TTL (60s).
    now.set(TimeUnit.SECONDS.toNanos(61));

    // A miss on a different key triggers the passive sweep.
    cache.checkoutInternal(KEY_B, () -> dsB);

    verify(dsA, times(1)).close();
    LanceDatasetCache.Stats s = cache.snapshotInternal();
    assertThat(s.evictions).isEqualTo(1);
    assertThat(s.size).isEqualTo(1); // only KEY_B remains
  }

  @Test
  @DisplayName("in-use entry (refcount > 0) is not evicted even after TTL")
  void eviction_skipsInUseEntry() {
    Dataset dsA = mock(Dataset.class);
    Dataset dsB = mock(Dataset.class);
    cache.checkoutInternal(KEY_A, () -> dsA);
    // Do NOT release -> refCount stays at 1.

    now.set(TimeUnit.SECONDS.toNanos(120));
    cache.checkoutInternal(KEY_B, () -> dsB); // triggers sweep

    verify(dsA, never()).close();
    LanceDatasetCache.Stats s = cache.snapshotInternal();
    assertThat(s.evictions).isEqualTo(0);
    assertThat(s.size).isEqualTo(2);
  }

  @Test
  @DisplayName("concurrent checkout of same key runs loader exactly once")
  void concurrentCheckout_loaderInvokedOnce() throws Exception {
    Dataset ds = mock(Dataset.class);
    AtomicInteger loaded = new AtomicInteger();
    Supplier<Dataset> loader =
        () -> {
          loaded.incrementAndGet();
          return ds;
        };

    int threads = 16;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    Set<Dataset> returned = ConcurrentHashMap.newKeySet();
    try {
      for (int i = 0; i < threads; i++) {
        pool.submit(
            () -> {
              try {
                start.await();
                returned.add(cache.checkoutInternal(KEY_A, loader));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      pool.shutdownNow();
    }

    assertThat(loaded.get()).isEqualTo(1);
    assertThat(returned).hasSize(1).containsExactly(ds);
    LanceDatasetCache.Stats s = cache.snapshotInternal();
    assertThat(s.hits + s.misses).isEqualTo(threads);
    assertThat(s.misses).isEqualTo(1);
    assertThat(s.hits).isEqualTo(threads - 1L);
    assertThat(s.size).isEqualTo(1);
  }

  @Test
  @DisplayName("different cache keys produce isolated entries")
  void differentKeys_isolated() {
    Dataset dsA = mock(Dataset.class);
    Dataset dsB = mock(Dataset.class);
    Dataset dsC = mock(Dataset.class);
    Dataset dsD = mock(Dataset.class);

    cache.checkoutInternal(new LanceDatasetCache.CacheKey("uri-1", 1, "c", 0), () -> dsA);
    cache.checkoutInternal(new LanceDatasetCache.CacheKey("uri-2", 1, "c", 0), () -> dsB);
    cache.checkoutInternal(new LanceDatasetCache.CacheKey("uri-1", 2, "c", 0), () -> dsC);
    cache.checkoutInternal(new LanceDatasetCache.CacheKey("uri-1", 1, "c2", 0), () -> dsD);

    assertThat(cache.snapshotInternal().size).isEqualTo(4);
    assertThat(cache.snapshotInternal().misses).isEqualTo(4);
    assertThat(cache.snapshotInternal().hits).isEqualTo(0);
  }

  @Test
  @DisplayName("clear closes all datasets and resets counters")
  void clear_closesAllAndResets() {
    Dataset dsA = mock(Dataset.class);
    Dataset dsB = mock(Dataset.class);
    cache.checkoutInternal(KEY_A, () -> dsA);
    cache.checkoutInternal(KEY_B, () -> dsB);

    cache.clearInternal();

    verify(dsA, times(1)).close();
    verify(dsB, times(1)).close();
    LanceDatasetCache.Stats s = cache.snapshotInternal();
    assertThat(s.size).isEqualTo(0);
    assertThat(s.hits).isEqualTo(0);
    assertThat(s.misses).isEqualTo(0);
    assertThat(s.evictions).isEqualTo(0);
  }

  @Test
  @DisplayName("CacheKey equals/hashCode are structural (legacy int-hash constructor)")
  void cacheKey_structuralEquality() {
    LanceDatasetCache.CacheKey k1 = new LanceDatasetCache.CacheKey("u", 3, "c", 7);
    LanceDatasetCache.CacheKey k2 = new LanceDatasetCache.CacheKey("u", 3, "c", 7);
    LanceDatasetCache.CacheKey k3 = new LanceDatasetCache.CacheKey("u", 4, "c", 7);

    assertThat(k1).isEqualTo(k2).hasSameHashCodeAs(k2);
    assertThat(k1).isNotEqualTo(k3);
    assertThat(k1.version()).isEqualTo(3);
    assertThat(k1.uri()).isEqualTo("u");
    assertThat(k1.catalogName()).isEqualTo("c");
    // Legacy constructor ignores its int hash and stores an empty storage-options snapshot.
    assertThat(k1.storageOptions()).isEmpty();
  }

  @Test
  @DisplayName(
      "CacheKey equality compares storage-options map by value, not hash (cross-tenant safety)")
  void cacheKey_crossTenantIsolation() {
    java.util.Map<String, String> tenantA = new java.util.HashMap<>();
    tenantA.put("aws.access_key_id", "AKIA-TENANT-A");
    tenantA.put("aws.secret_access_key", "secret-A");

    java.util.Map<String, String> tenantB = new java.util.HashMap<>();
    tenantB.put("aws.access_key_id", "AKIA-TENANT-B");
    tenantB.put("aws.secret_access_key", "secret-B");

    LanceDatasetCache.CacheKey kA =
        new LanceDatasetCache.CacheKey("s3://shared-bucket/ds", 1, "cat", tenantA);
    LanceDatasetCache.CacheKey kB =
        new LanceDatasetCache.CacheKey("s3://shared-bucket/ds", 1, "cat", tenantB);

    // Same URI+version+catalog, different credentials — MUST NOT be equal even if
    // TreeMap.hashCode() were to collide (which it cannot for these two maps, but the
    // stronger guarantee is that equals() is no longer derived from an int hash at all).
    assertThat(kA).isNotEqualTo(kB);
  }

  @Test
  @DisplayName(
      "CacheKey equal when storage-options maps differ in iteration order but same entries")
  void cacheKey_orderIndependentMapEquality() {
    java.util.Map<String, String> m1 = new java.util.LinkedHashMap<>();
    m1.put("region", "us-east-1");
    m1.put("endpoint", "https://s3.amazonaws.com");

    java.util.Map<String, String> m2 = new java.util.LinkedHashMap<>();
    m2.put("endpoint", "https://s3.amazonaws.com");
    m2.put("region", "us-east-1");

    LanceDatasetCache.CacheKey k1 = new LanceDatasetCache.CacheKey("s3://b/ds", 7, "c", m1);
    LanceDatasetCache.CacheKey k2 = new LanceDatasetCache.CacheKey("s3://b/ds", 7, "c", m2);

    assertThat(k1).isEqualTo(k2).hasSameHashCodeAs(k2);
  }

  @Test
  @DisplayName("CacheKey snapshot is defensively copied — caller mutation does not affect key")
  void cacheKey_storageOptionsSnapshotIsDefensive() {
    java.util.Map<String, String> original = new java.util.HashMap<>();
    original.put("k", "v");

    LanceDatasetCache.CacheKey key = new LanceDatasetCache.CacheKey("uri", 1, "cat", original);

    // Mutating the original map after construction must not change the key's snapshot.
    original.put("k", "MUTATED");
    original.put("added", "value");

    assertThat(key.storageOptions()).hasSize(1).containsEntry("k", "v");
    assertThat(key.storageOptions()).doesNotContainKey("added");
  }

  @Test
  @DisplayName("release on missing key is a no-op (tolerates double-release)")
  void release_missingKey_noop() {
    // Should not throw.
    cache.releaseInternal(KEY_A);
    assertThat(cache.snapshotInternal().size).isEqualTo(0);
  }

  @Test
  @DisplayName("re-checkout after release within TTL reuses cached dataset (no re-load)")
  void recheckout_withinTtl_reusesDataset() {
    Dataset ds = mock(Dataset.class);
    AtomicInteger loaded = new AtomicInteger();
    Supplier<Dataset> loader =
        () -> {
          loaded.incrementAndGet();
          return ds;
        };
    cache.checkoutInternal(KEY_A, loader);
    cache.releaseInternal(KEY_A);

    now.set(TimeUnit.SECONDS.toNanos(30)); // within 60s TTL

    Dataset again = cache.checkoutInternal(KEY_A, loader);
    assertThat(again).isSameAs(ds);
    assertThat(loaded.get()).isEqualTo(1);
    verify(ds, never()).close();
  }

  @Test
  @DisplayName("snapshot values accumulate across mixed checkout patterns")
  void snapshot_accumulates() {
    List<Dataset> datasets = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Dataset ds = mock(Dataset.class);
      datasets.add(ds);
      cache.checkoutInternal(new LanceDatasetCache.CacheKey("u-" + i, 1, "c", 0), () -> ds);
    }
    // 2 hits on first key
    Dataset ds0 = datasets.get(0);
    cache.checkoutInternal(new LanceDatasetCache.CacheKey("u-0", 1, "c", 0), () -> ds0);
    cache.checkoutInternal(new LanceDatasetCache.CacheKey("u-0", 1, "c", 0), () -> ds0);

    LanceDatasetCache.Stats s = cache.snapshotInternal();
    assertThat(s.misses).isEqualTo(3);
    assertThat(s.hits).isEqualTo(2);
    assertThat(s.size).isEqualTo(3);
  }
}
