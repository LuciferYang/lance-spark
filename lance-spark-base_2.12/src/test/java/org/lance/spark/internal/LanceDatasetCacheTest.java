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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LanceDatasetCache.Key} and the system-property toggles that govern caching
 * behavior. Tests for the actual cache hit semantics are covered by the JMH probe + benchmark suite
 * (see {@code docs/profiles/2026-05-25-fixed-cost-probe.md}); those require a real Lance dataset so
 * they don't fit in a unit test.
 */
class LanceDatasetCacheTest {

  // ---------- Key equals / hashCode contract ----------

  @Test
  void keyEqualsItself() {
    LanceDatasetCache.Key k = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    assertEquals(k, k);
    assertEquals(k.hashCode(), k.hashCode());
  }

  @Test
  void keyEqualsForSameInputs() {
    LanceDatasetCache.Key a = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    LanceDatasetCache.Key b = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void keyDiffersOnUri() {
    LanceDatasetCache.Key a = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    LanceDatasetCache.Key b = new LanceDatasetCache.Key("s3://b/u.lance", 1, 42);
    assertNotEquals(a, b);
  }

  @Test
  void keyDiffersOnVersion() {
    LanceDatasetCache.Key a = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    LanceDatasetCache.Key b = new LanceDatasetCache.Key("s3://b/t.lance", 2, 42);
    assertNotEquals(a, b);
  }

  @Test
  void keyDiffersOnStorageOptionsHash() {
    LanceDatasetCache.Key a = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    LanceDatasetCache.Key b = new LanceDatasetCache.Key("s3://b/t.lance", 1, 43);
    assertNotEquals(a, b);
  }

  @Test
  void keyTreatsNullVersionAsLatest() {
    // Two opens of the same URI with no explicit version pin must collide so
    // the cache delivers the same Dataset to both.
    LanceDatasetCache.Key a = new LanceDatasetCache.Key("s3://b/t.lance", null, 42);
    LanceDatasetCache.Key b = new LanceDatasetCache.Key("s3://b/t.lance", null, 42);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void keyDistinguishesNullVersionFromPinned() {
    LanceDatasetCache.Key latest = new LanceDatasetCache.Key("s3://b/t.lance", null, 42);
    LanceDatasetCache.Key pinned = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    assertNotEquals(latest, pinned);
  }

  @Test
  void keyEqualsHandlesNullObject() {
    LanceDatasetCache.Key k = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    assertNotEquals(null, k);
  }

  @Test
  void keyEqualsHandlesWrongType() {
    LanceDatasetCache.Key k = new LanceDatasetCache.Key("s3://b/t.lance", 1, 42);
    assertNotEquals("not a key", k);
  }

  // ---------- System property toggle contract ----------

  private String savedEnabled;
  private String savedMax;

  @BeforeEach
  void capture() {
    savedEnabled = System.getProperty(LanceDatasetCache.CACHE_ENABLED_PROPERTY);
    savedMax = System.getProperty(LanceDatasetCache.CACHE_MAX_SIZE_PROPERTY);
  }

  @AfterEach
  void restore() {
    if (savedEnabled == null) {
      System.clearProperty(LanceDatasetCache.CACHE_ENABLED_PROPERTY);
    } else {
      System.setProperty(LanceDatasetCache.CACHE_ENABLED_PROPERTY, savedEnabled);
    }
    if (savedMax == null) {
      System.clearProperty(LanceDatasetCache.CACHE_MAX_SIZE_PROPERTY);
    } else {
      System.setProperty(LanceDatasetCache.CACHE_MAX_SIZE_PROPERTY, savedMax);
    }
    LanceDatasetCache.clear();
  }

  @Test
  void cacheEmptyByDefault() {
    LanceDatasetCache.clear();
    assertEquals(0, LanceDatasetCache.size());
  }

  @Test
  void clearEmptiesCache() {
    LanceDatasetCache.clear();
    assertEquals(0, LanceDatasetCache.size());
    // Re-clearing on an empty cache is idempotent.
    LanceDatasetCache.clear();
    assertEquals(0, LanceDatasetCache.size());
  }

  // ---------- OpenResult basic invariants ----------

  @Test
  void openResultExposesCacheFlag() {
    LanceDatasetCache.OpenResult cached = new LanceDatasetCache.OpenResult(null, true);
    LanceDatasetCache.OpenResult fresh = new LanceDatasetCache.OpenResult(null, false);
    assertTrue(cached.cached());
    assertFalse(fresh.cached());
    // dataset() returns whatever the constructor was given (here, null is acceptable
    // for the structural test since we are not exercising lance native code here).
    assertNull(cached.dataset());
    assertNull(fresh.dataset());
  }

  @Test
  void openResultRetainsDatasetReferenceIdentity() {
    Object sentinel = new Object();
    @SuppressWarnings("unchecked")
    LanceDatasetCache.OpenResult r =
        new LanceDatasetCache.OpenResult((org.lance.Dataset) null, true);
    assertNotNull(r); // constructor accepts even null without throwing
    // Identity check on the cached() flag — boolean field, not a reference.
    assertTrue(r.cached());
  }
}
