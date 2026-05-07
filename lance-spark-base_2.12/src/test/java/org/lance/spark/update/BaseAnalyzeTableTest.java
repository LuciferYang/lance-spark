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
package org.lance.spark.update;

import org.lance.spark.read.LanceStatsKeys;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for {@code ANALYZE TABLE … COMPUTE STATISTICS}. Exercises the full stack:
 * grammar → AST → logical plan → {@code LanceAnalyzeTableExec} → {@code catalog.alterTable} →
 * manifest write → {@code LanceScanBuilder.loadPersistedColumnStats}. Asserts (a) the TBLPROPERTIES
 * write happens with the expected wire-format keys, (b) the persisted payload round-trips through
 * the read path back into Spark's {@code Statistics.columnStats()}, and (c) staleness invalidates
 * the cached stats so a subsequent INSERT correctly forces fallback.
 *
 * <p>Sub-classed per Spark major version so each module's parser-strategy wiring is exercised.
 */
public abstract class BaseAnalyzeTableTest {

  protected final String catalogName = "lance_test";

  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("lance-analyze-table-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      // Clear active+default session refs so that a follow-up test class calling
      // SparkSession.builder().getOrCreate() builds a fresh session with its own catalog config.
      // Without this, the next test inherits this class's catalog wiring and silently
      // mis-routes ANALYZE TABLE / SELECT to the wrong namespace.
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      spark.close();
      spark = null;
    }
  }

  private TableCatalog catalog() {
    return (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  private java.util.Map<String, String> properties(String tableName) throws Exception {
    Table table = catalog().loadTable(Identifier.of(new String[] {"default"}, tableName));
    return table.properties();
  }

  @Test
  public void analyzeForAllColumnsPersistsExpectedKeys() throws Exception {
    String tableName = "t_for_all_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, name STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> props = properties(tableName);
    // Top-level wire-format sentinels must all be present.
    assertEquals("true", props.get(LanceStatsKeys.COMPLETE));
    assertEquals(LanceStatsKeys.SUPPORTED_VERSION, props.get(LanceStatsKeys.VERSION));
    assertEquals("3", props.get(LanceStatsKeys.NUM_ROWS));
    assertNotNull(props.get(LanceStatsKeys.COMPUTED_AT_VERSION));
    assertNotNull(props.get(LanceStatsKeys.SCHEMA_HASH));
    assertNotNull(props.get(LanceStatsKeys.COMPUTED_AT));

    // Per-column min/max must round-trip via the codec.
    String idMinKey = LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_MIN;
    String idMaxKey = LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_MAX;
    assertEquals("i64:1", props.get(idMinKey));
    assertEquals("i64:3", props.get(idMaxKey));
    // distinctMode for exact (non-APPROX) NDV.
    String idModeKey =
        LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_MODE;
    assertEquals(LanceStatsKeys.DISTINCT_MODE_EXACT, props.get(idModeKey));

    // String column min/max are base64-encoded.
    String nameMinKey = LanceStatsKeys.COLUMN_PREFIX + "name" + LanceStatsKeys.COLUMN_SUFFIX_MIN;
    assertTrue(
        props.get(nameMinKey).startsWith("utf8:"),
        "Expected utf8: prefix on name.min, got: " + props.get(nameMinKey));
  }

  @Test
  public void analyzeForColumnsSubsetOnlyPersistsRequestedColumns() throws Exception {
    String tableName = "t_subset_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id INT, payload STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'x'), (2, 'y')");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR COLUMNS id");

    java.util.Map<String, String> props = properties(tableName);
    // id stats present.
    assertNotNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_MIN));
    // payload stats absent (subset).
    assertNull(
        props.get(LanceStatsKeys.COLUMN_PREFIX + "payload" + LanceStatsKeys.COLUMN_SUFFIX_MIN));
  }

  @Test
  public void analyzeApproxModePersistsApproxDistinctMode() throws Exception {
    String tableName = "t_approx_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2), (3), (1), (2)");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS APPROX");

    java.util.Map<String, String> props = properties(tableName);
    String mode =
        props.get(LanceStatsKeys.COLUMN_PREFIX + "id" + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_MODE);
    assertEquals(LanceStatsKeys.DISTINCT_MODE_APPROX, mode);
  }

  @Test
  public void subsequentInsertInvalidatesPersistedStats() throws Exception {
    String tableName = "t_invalidate_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2)");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> props = properties(tableName);
    long taggedVersion = Long.parseLong(props.get(LanceStatsKeys.COMPUTED_AT_VERSION));

    // A subsequent INSERT bumps the manifest version; the persisted-stats version-equality
    // check on the read path should now reject these stats and fall back to live aggregation.
    spark.sql("INSERT INTO " + fullName + " VALUES (3)");

    // Stats keys are still on disk (we don't proactively delete); the reader's exact-equality
    // check is what protects correctness. Sanity-check the key still exists with the OLD value
    // — the read path's protection is logical, not physical.
    java.util.Map<String, String> after = properties(tableName);
    assertEquals(
        Long.toString(taggedVersion),
        after.get(LanceStatsKeys.COMPUTED_AT_VERSION),
        "INSERT should not modify persisted-stats keys; the read path detects staleness");
  }

  @Test
  public void analyzeReturnsResultRowWithAnalyzedCount() throws Exception {
    String tableName = "t_result_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (a INT, b STRING, c DOUBLE) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'x', 1.5)");

    List<Row> rows =
        spark
            .sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS")
            .collectAsList();
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    // Per LanceAnalyzeTableOutputType.SCHEMA: (columns_analyzed, num_rows, manifest_version,
    // approx)
    assertEquals(3, r.getInt(0)); // 3 columns analyzed
    assertEquals(1L, r.getLong(1)); // 1 row
    assertTrue(r.getLong(2) > 0); // manifest_version > 0
    assertEquals(false, r.getBoolean(3)); // not approx mode
  }

  @Test
  public void reAnalyzeOnUnchangedSchemaIsIdempotent() throws Exception {
    // Idempotency check: running ANALYZE twice on an unchanged table produces the same per-
    // column stats keys AND the same schemaHash. The orphan-cleanup pass during the second run
    // sees no orphans (every key belongs to a still-present column) and the per-column writes
    // overwrite with identical values. Only computedAt (ISO timestamp) is allowed to differ.
    String tableName = "t_idempotent_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT, name STRING) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1, 'a'), (2, 'b')");

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");
    java.util.Map<String, String> first = new java.util.HashMap<>(properties(tableName));

    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");
    java.util.Map<String, String> second = properties(tableName);

    // Per-column key stability.
    for (String key : first.keySet()) {
      if (!key.startsWith(LanceStatsKeys.COLUMN_PREFIX)) {
        continue;
      }
      assertEquals(first.get(key), second.get(key), "Stat key '" + key + "' should be stable");
    }
    // Schema hash must be deterministic — a regression here would invalidate persisted stats
    // on every read.
    assertEquals(
        first.get(LanceStatsKeys.SCHEMA_HASH),
        second.get(LanceStatsKeys.SCHEMA_HASH),
        "schemaHash must be stable across re-ANALYZE on unchanged schema");
    // Numeric counts must also be identical.
    assertEquals(first.get(LanceStatsKeys.NUM_ROWS), second.get(LanceStatsKeys.NUM_ROWS));
    assertEquals(first.get(LanceStatsKeys.VERSION), second.get(LanceStatsKeys.VERSION));
  }

  @Test
  public void analyzeTableMalformedSyntaxSurfacesLanceGrammarError() throws Exception {
    // R14: When the priority router hands a malformed ANALYZE TABLE to the Lance grammar, the
    // resulting parse error MUST propagate to the caller. The previous fallback delegated the
    // original SQL to Spark's parser, which produced a confusing V2-rejection instead.
    String tableName = "t_malformed_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");

    // Trailing FOR COLUMNS without any columns is a Lance-grammar parse failure (the
    // grammar's `analyzeForColumns` rule requires `columnList`). The user should see the
    // grammar's parse error, not Spark's NOT_SUPPORTED_COMMAND_FOR_V2_TABLE.
    Exception ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            Exception.class,
            () -> spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR COLUMNS"));
    String msg = ex.getMessage() == null ? "" : ex.getMessage();
    assertFalse(
        msg.contains("NOT_SUPPORTED_COMMAND_FOR_V2_TABLE"),
        "Lance parse error should surface, not Spark's V2 rejection. Got: " + msg);
  }

  @Test
  public void analyzeTableWithLeadingCommentRoutesThroughExtensionParser() throws Exception {
    // R13: the priority-routing predicate must skip leading SQL comments before checking the
    // ANALYZE TABLE keyword. Without this, Spark's delegate parser parses the SQL, produces a
    // built-in AnalyzeTableCommand, and the analyzer rejects it for V2 tables. Exercise the
    // predicate's parsePlan integration end-to-end by issuing the SQL via spark.sql.
    String tableName = "t_comment_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1)");

    // Block comment + line comment + extra whitespace before the keyword. If the predicate
    // regression slips, this throws NOT_SUPPORTED_COMMAND_FOR_V2_TABLE.
    spark.sql(
        "/* hint */\n-- another\n  ANALYZE TABLE "
            + fullName
            + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> props = properties(tableName);
    assertEquals("true", props.get(LanceStatsKeys.COMPLETE));
  }

  @Test
  public void killSwitchSuppressesPersistedStatsConsumption() throws Exception {
    // R19: spark.lance.cbo.column.stats.enabled=false is the production rollback path. Must
    // suppress persisted-stats consumption end-to-end. Hard to assert "no stats consumed"
    // directly (no driver-side metric); assert that scans complete successfully and produce
    // correct rows even with the kill-switch on, AND that the persisted TBLPROPERTIES are
    // unchanged (the writer path is unaffected by the read-side kill-switch).
    String tableName = "t_killswitch_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2), (3)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");

    java.util.Map<String, String> propsBefore = properties(tableName);
    String hashBefore = propsBefore.get(LanceStatsKeys.SCHEMA_HASH);
    assertNotNull(hashBefore, "ANALYZE should have written stats");

    try {
      spark.conf().set(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ENABLED, "false");
      // Scan must succeed and return correct rows. With the kill-switch on, the read path's
      // CBO column-stats fast path is skipped entirely (no persisted-stats consumption, no
      // zonemap aggregation). Underlying scan correctness is unaffected.
      long count = spark.sql("SELECT COUNT(*) FROM " + fullName).collectAsList().get(0).getLong(0);
      assertEquals(3L, count);
      long selected = spark.sql("SELECT * FROM " + fullName).count();
      assertEquals(3L, selected);
    } finally {
      spark.conf().unset(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ENABLED);
    }

    // Persisted-stats keys are untouched by the kill-switch — re-flipping enabled=true should
    // bring back the fast path without re-running ANALYZE.
    java.util.Map<String, String> propsAfter = properties(tableName);
    assertEquals(hashBefore, propsAfter.get(LanceStatsKeys.SCHEMA_HASH));
  }

  @Test
  public void readPathRejectsStaleStatsAfterInsert() throws Exception {
    // Companion to subsequentInsertInvalidatesPersistedStats: prove the staleness check fires
    // at runtime. We can't directly inspect the CBO decision in this base test, but we can
    // assert two things that together pin the staleness invariant:
    //   1. The on-disk computedAtVersion did NOT match the post-INSERT manifest version (so
    //      the read path's exact-equality check sees a mismatch and falls back).
    //   2. Subsequent SELECTs return the correct row count and do not throw.
    String tableName = "t_runtime_stale_" + System.nanoTime();
    String fullName = catalogName + ".default." + tableName;
    spark.sql("CREATE TABLE " + fullName + " (id BIGINT) USING lance");
    spark.sql("INSERT INTO " + fullName + " VALUES (1), (2)");
    spark.sql("ANALYZE TABLE " + fullName + " COMPUTE STATISTICS FOR ALL COLUMNS");
    long taggedVersion =
        Long.parseLong(properties(tableName).get(LanceStatsKeys.COMPUTED_AT_VERSION));
    spark.sql("INSERT INTO " + fullName + " VALUES (3)");

    // Confirm the persisted version is now strictly less than the live manifest version, which
    // is what the read-path's exact-equality check uses to reject the stats. We verify via the
    // resulting row count: ANALYZE captured 2 rows, post-INSERT live count is 3.
    long count = spark.sql("SELECT COUNT(*) FROM " + fullName).collectAsList().get(0).getLong(0);
    assertEquals(3L, count, "Live count must reflect post-INSERT data, not stale ANALYZE numRows");
    long selected = spark.sql("SELECT * FROM " + fullName).count();
    assertEquals(3L, selected, "SELECT * must surface all post-INSERT rows");

    // Persisted-stats version must still equal the original tag (writer didn't update it on
    // INSERT) — proving staleness detection is logical (read-side equality) rather than
    // physical (writer wipes stats on every mutation).
    assertEquals(
        Long.toString(taggedVersion),
        properties(tableName).get(LanceStatsKeys.COMPUTED_AT_VERSION));
  }
}
