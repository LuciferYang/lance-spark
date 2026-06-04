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
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * End-to-end acceptance test for issue #510 / PR #511: a Lance scan must expose per-column {@link
 * org.apache.spark.sql.connector.read.colstats.ColumnStatistics} derived from a <b>standalone
 * zonemap index</b> (the {@code CREATE INDEX ... USING zonemap} path shipped in #516), and Spark's
 * cost-based optimizer must actually consume them.
 *
 * <p>This is intentionally <b>black-box</b>: it drives only SQL DDL, the public {@code
 * spark.lance.cbo.column.stats.*} configs, and Spark's optimized-plan statistics. It has no
 * compile-time dependency on {@code ColumnStatsAggregator} / {@code LanceStatistics.columnStats()}.
 * Consequence: it is expected to be <b>RED on {@code main}</b> (where the connector reports no
 * column stats, so the kill-switch is a no-op and the two row-count estimates are equal) and
 * <b>GREEN once #511 lands</b>. That is the point — it is the falsifiable gate that proves Spark
 * ingests zonemap-derived stats, which #511's unit tests ({@code ColumnStatsAggregatorTest}) do
 * not.
 *
 * <p>Why row-count deltas instead of parsing {@code EXPLAIN COST}: column-stat rendering in {@code
 * EXPLAIN COST} is version-dependent and brittle. The optimizer's own {@code rowCount} estimate for
 * a range predicate is the most stable observable proof that min/max stats were consumed by {@code
 * FilterEstimation}. With CBO on and {@code id} bounded to {@code [0, MAX_ID]}, a predicate {@code
 * id > MAX_ID*1000} estimates ~0 rows when min/max are known, but falls back to the default {@code
 * >} selectivity (~1/3 of rows) when they are not.
 */
public abstract class BaseZonemapColumnStatsTest {

  /**
   * Inclusive upper bound of the inserted {@code id} range; the impossible predicate sits above.
   */
  private static final int MAX_ID = 199;

  protected String catalogName = "lance_test";
  protected String tableName;
  protected String fullTable;

  protected SparkSession spark;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-zonemap-column-stats-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            // CBO must be on for the optimizer to estimate from column stats at all.
            .config("spark.sql.cbo.enabled", "true")
            .config("spark.sql.cbo.joinReorder.enabled", "true")
            .getOrCreate();
    this.tableName = "zonemap_colstats_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = this.catalogName + ".default." + this.tableName;
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  /**
   * Insert {@code id} in {@code [0, MAX_ID]} spread across {@code fragments} fragments (one INSERT
   * per fragment), so the table has multiple fragments / zones — the realistic shape a distributed
   * zonemap build segments over.
   */
  private void prepareDataset(int fragments) {
    spark.sql(String.format("create table %s (id int, val string) using lance", fullTable));
    int total = MAX_ID + 1;
    int perFragment = (int) Math.ceil((double) total / fragments);
    for (int f = 0; f < fragments; f++) {
      int lo = f * perFragment;
      int hi = Math.min(lo + perFragment, total);
      if (lo >= hi) {
        break;
      }
      String values =
          IntStream.range(lo, hi)
              .mapToObj(i -> String.format("(%d, 'v_%d')", i, i))
              .collect(Collectors.joining(","));
      spark.sql(String.format("insert into %s (id, val) values %s", fullTable, values));
    }
  }

  /** The optimizer's estimated row count for {@code sql}, or {@code Long.MAX_VALUE} if unknown. */
  private long estimatedRowCount(String sql) {
    Statistics stats = spark.sql(sql).queryExecution().optimizedPlan().stats();
    scala.Option<scala.math.BigInt> rc = stats.rowCount();
    return rc.isDefined() ? rc.get().longValue() : Long.MAX_VALUE;
  }

  /** A predicate that can never match (above the column max), so CBO with min/max estimates ~0. */
  private String impossibleRangeQuery() {
    return String.format("select * from %s where id > %d", fullTable, MAX_ID * 1000L);
  }

  /**
   * Core gap: a standalone {@code USING zonemap} index (no btree) must feed min/max to CBO, and the
   * kill-switch must turn that off. Proves #511 reads stats off the #516 zonemap creation path —
   * not just btree's embedded zonemap.
   */
  @Test
  public void zonemapOnlyIndexFeedsMinMaxToCbo() {
    prepareDataset(4);
    spark.sql(String.format("alter table %s create index idx_id using zonemap (id)", fullTable));

    String query = impossibleRangeQuery();

    // Kill-switch OFF (feature ON, the default): min/max known -> impossible predicate ~ 0 rows.
    spark.conf().set("spark.lance.cbo.column.stats.enabled", "true");
    long withStats = estimatedRowCount(query);

    // Kill-switch ON (feature OFF): no column stats -> default '>' selectivity (~1/3 of rows).
    spark.conf().set("spark.lance.cbo.column.stats.enabled", "false");
    long withoutStats = estimatedRowCount(query);

    Assertions.assertTrue(
        withStats < withoutStats,
        String.format(
            "Expected zonemap min/max to tighten the CBO estimate: withStats(%d) < withoutStats(%d)."
                + " Equality means Spark did not consume the connector's columnStats() — the #510"
                + " goal is unmet on this Spark version.",
            withStats, withoutStats));
    Assertions.assertTrue(
        withStats <= 1,
        String.format(
            "An impossible predicate (id > %d, max id = %d) should estimate ~0 rows when min/max"
                + " are known, but got %d.",
            MAX_ID * 1000L, MAX_ID, withStats));
  }

  /**
   * The new variable in #516 is <b>segmentation</b>: a zonemap built with {@code num_segments > 1}
   * commits multiple per-fragment segments under one logical index. {@code getZonemapStats()} must
   * still return complete per-zone stats across those segments for the CBO estimate to hold.
   */
  @Test
  public void segmentedZonemapIndexStillFeedsColumnStats() {
    prepareDataset(4);
    spark.sql(
        String.format(
            "alter table %s create index idx_id using zonemap (id) with (num_segments = 3)",
            fullTable));

    String query = impossibleRangeQuery();

    spark.conf().set("spark.lance.cbo.column.stats.enabled", "true");
    long withStats = estimatedRowCount(query);
    spark.conf().set("spark.lance.cbo.column.stats.enabled", "false");
    long withoutStats = estimatedRowCount(query);

    Assertions.assertTrue(
        withStats < withoutStats,
        String.format(
            "Segmented (num_segments=3) zonemap min/max must still reach CBO:"
                + " withStats(%d) < withoutStats(%d). Failure here means getZonemapStats() does not"
                + " aggregate across #516 segments.",
            withStats, withoutStats));
  }
}
