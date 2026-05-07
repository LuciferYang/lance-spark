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
package org.lance.spark.read;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LanceScanBuilder#loadPersistedColumnStats} fast-path branches. The fast
 * path is the entire premise of ANALYZE TABLE persistent stats; a regression in any branch silently
 * degrades CBO without a failing test, so we test each guard independently rather than relying on
 * end-to-end coverage alone.
 *
 * <p>Tests pass {@code session = null} since the only session use is the {@code allowStale}
 * SparkConf lookup, which gracefully falls through to {@code allowStale=false} when no session is
 * bound.
 */
class LoadPersistedColumnStatsTest {

  /**
   * Local SparkSession used by the {@code allowStale} branch tests. Built once for the class so the
   * per-test cost stays low; the rest of the tests pass {@code session = null} since they don't
   * exercise SparkConf.
   */
  private static SparkSession spark;

  @BeforeAll
  static void setUpSession() {
    spark =
        SparkSession.builder()
            .appName("LoadPersistedColumnStatsTest")
            .master("local")
            .getOrCreate();
  }

  @AfterAll
  static void tearDownSession() {
    if (spark != null) {
      // Clear active+default refs: SparkSession.builder().getOrCreate() in a sibling test class
      // returns this thread's active session if one is still bound, silently inheriting our
      // (non-catalog-configured) builder.
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      spark.stop();
      spark = null;
    }
  }

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.LongType, true, null),
            new StructField("name", DataTypes.StringType, true, null)
          });

  /** Build a properties map representing a fully-valid v1 stats payload at version=42. */
  private static Map<String, String> validProps() {
    Map<String, String> p = new HashMap<>();
    p.put(LanceStatsKeys.COMPLETE, "true");
    p.put(LanceStatsKeys.VERSION, LanceStatsKeys.SUPPORTED_VERSION);
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "42");
    p.put(LanceStatsKeys.SCHEMA_HASH, LanceStatsKeys.computeSchemaHash(SCHEMA));
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_MIN, "i64:1");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_MAX, "i64:100");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_NULL_COUNT, "5");
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_COUNT, "95");
    return p;
  }

  @Test
  @DisplayName("Valid payload returns ColumnStatistics for analyzed column")
  void validPayloadReturnsStats() {
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, validProps(), SCHEMA, SCHEMA, 42L);
    assertNotNull(result);
    NamedReference idRef = FieldReference.column("id");
    assertTrue(result.containsKey(idRef));
    ColumnStatistics idStats = result.get(idRef);
    assertEquals(1L, idStats.min().get());
    assertEquals(100L, idStats.max().get());
    assertEquals(5L, idStats.nullCount().getAsLong());
    assertEquals(95L, idStats.distinctCount().getAsLong());
  }

  @Test
  @DisplayName("complete=false rejects payload (atomicity sentinel)")
  void completeFalseRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COMPLETE, "false");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("complete absent rejects payload")
  void completeAbsentRejected() {
    Map<String, String> p = validProps();
    p.remove(LanceStatsKeys.COMPLETE);
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("complete is case-insensitive (\"True\" → accepted)")
  void completeIsCaseInsensitive() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COMPLETE, "True");
    assertNotNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("formatVersion absent → fall back (R8 fail-safe gate)")
  void formatVersionAbsentRejected() {
    Map<String, String> p = validProps();
    p.remove(LanceStatsKeys.VERSION);
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("formatVersion=2 (unrecognized) → fall back")
  void formatVersionUnrecognizedRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.VERSION, "2");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("computedAtVersion mismatch + allowStale unset → fall back")
  void staleVersionRejected() {
    // currentManifestVersion=43 but stats are tagged at version=42 — must reject without an
    // active session bound (so allowStale defaults to false).
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, validProps(), SCHEMA, SCHEMA, 43L));
  }

  @Test
  @DisplayName("computedAtVersion malformed → fall back")
  void computedAtVersionMalformedRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "not-a-number");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("computedAtVersion negative → fall back")
  void computedAtVersionNegativeRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COMPUTED_AT_VERSION, "-1");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("schemaHash mismatch → fall back (drift guard)")
  void schemaHashMismatchRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.SCHEMA_HASH, "0".repeat(64)); // not the SCHEMA's hash
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("schemaHash absent → fall back (R9 fail-safe — was silent pass-through pre-R9)")
  void schemaHashAbsentRejected() {
    Map<String, String> p = validProps();
    p.remove(LanceStatsKeys.SCHEMA_HASH);
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("Empty properties → null (no stats to consume)")
  void emptyPropsReturnsNull() {
    assertNull(
        LanceScanBuilder.loadPersistedColumnStats(null, new HashMap<>(), SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("Null properties → null")
  void nullPropsReturnsNull() {
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, null, SCHEMA, SCHEMA, 42L));
  }

  @Test
  @DisplayName("All-null skip: min present but decoded null + ndv absent → skip column")
  void minLostWithoutNdvSkips() {
    Map<String, String> p = validProps();
    // Replace id.min with an oversized base64 body that makes the codec return null.
    StringBuilder oversized = new StringBuilder("utf8:");
    for (int i = 0; i < 70_000; i++) {
      oversized.append('A');
    }
    p.put(
        LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_MIN,
        oversized.toString());
    // Drop NDV so the only signal is the broken min.
    p.remove(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_COUNT);
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L);
    // The column is skipped; if no other column has stats, result is null (per
    // result.isEmpty() ? null : result contract).
    assertNull(result);
  }

  @Test
  @DisplayName(
      "All-null skip: min present but decoded null + ndv present → still emit (NDV signal)")
  void minLostWithNdvKeeps() {
    Map<String, String> p = validProps();
    StringBuilder oversized = new StringBuilder("utf8:");
    for (int i = 0; i < 70_000; i++) {
      oversized.append('A');
    }
    p.put(
        LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_MIN,
        oversized.toString());
    // NDV present, so the column still has an optimizer-useful signal.
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L);
    assertNotNull(result);
    NamedReference idRef = FieldReference.column("id");
    ColumnStatistics idStats = result.get(idRef);
    assertNotNull(idStats);
    // Min unavailable, max valid, NDV valid.
    assertTrue(idStats.min().isEmpty() || idStats.min().get() == null);
    assertEquals(100L, idStats.max().get());
    assertEquals(95L, idStats.distinctCount().getAsLong());
  }

  @Test
  @DisplayName("Projected schema is a subset → only projected columns appear")
  void projectionFiltersStats() {
    // Add stats for "name" too, then project only "id".
    Map<String, String> p = validProps();
    p.put(
        LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsKeys.COLUMN_SUFFIX_MIN,
        "utf8:" + java.util.Base64.getEncoder().encodeToString("alice".getBytes()));
    StructType projected = new StructType(new StructField[] {SCHEMA.fields()[0]});
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, projected, 42L);
    assertNotNull(result);
    assertTrue(result.containsKey(FieldReference.column("id")));
    assertEquals(1, result.size(), "name should not appear in projected result");
  }

  @Test
  @DisplayName("Projected schema with no analyzed columns → null (caller falls back)")
  void projectedSchemaWithNoStatsReturnsNull() {
    // ANALYZE wrote stats for "id" only; project only "name". All sentinels pass but no
    // per-column entries match the projection, so the method returns null and the caller
    // falls back to live zonemap aggregation.
    StructType nameOnly = new StructType(new StructField[] {SCHEMA.fields()[1]});
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, validProps(), SCHEMA, nameOnly, 42L);
    assertNull(result);
  }

  @Test
  @DisplayName("Column with no stats keys at all is skipped (not a malformed-decode case)")
  void columnWithoutAnalyzeIsSkipped() {
    // "name" never had ANALYZE stats written; only "id" did. result should contain only "id".
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, validProps(), SCHEMA, SCHEMA, 42L);
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.containsKey(FieldReference.column("id")));
  }

  @Test
  @DisplayName("allowStale=true → accept stats whose computedAtVersion lags currentManifestVersion")
  void allowStaleAcceptsStaleVersion() {
    try {
      spark.conf().set(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE, "true");
      // computedAtVersion=42, currentManifestVersion=43 — stale, but allowStale=true accepts.
      Map<NamedReference, ColumnStatistics> result =
          LanceScanBuilder.loadPersistedColumnStats(spark, validProps(), SCHEMA, SCHEMA, 43L);
      assertNotNull(result, "allowStale=true should accept stale stats");
      assertTrue(result.containsKey(FieldReference.column("id")));
    } finally {
      spark.conf().unset(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE);
    }
  }

  @Test
  @DisplayName("allowStale=false (explicit) → still reject stale version")
  void allowStaleFalseStillRejectsStale() {
    try {
      spark.conf().set(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE, "false");
      Map<NamedReference, ColumnStatistics> result =
          LanceScanBuilder.loadPersistedColumnStats(spark, validProps(), SCHEMA, SCHEMA, 43L);
      assertNull(result, "allowStale=false must reject stale stats");
    } finally {
      spark.conf().unset(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE);
    }
  }

  @Test
  @DisplayName("allowStale unset + session present → defaults to false (rejects stale)")
  void allowStaleUnsetDefaultsToFalse() {
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(spark, validProps(), SCHEMA, SCHEMA, 43L);
    assertNull(result);
  }

  @Test
  @DisplayName("Negative nullCount is treated as malformed (range guard)")
  void negativeNullCountRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_NULL_COUNT, "-1");
    Map<NamedReference, ColumnStatistics> result =
        LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L);
    // The id column was the only analyzed column; negative nullCount makes its stats malformed
    // and the column is skipped → result map is empty → method returns null.
    assertNull(result);
  }

  @Test
  @DisplayName("Zero or negative distinctCount is treated as malformed (range guard)")
  void nonPositiveDistinctCountRejected() {
    Map<String, String> p = validProps();
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_COUNT, "0");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
    p.put(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_COUNT, "-5");
    assertNull(LanceScanBuilder.loadPersistedColumnStats(null, p, SCHEMA, SCHEMA, 42L));
  }
}
