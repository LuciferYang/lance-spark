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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseLimitPartitionPruningTest {
  private static SparkSession spark;
  private static final int NUM_FRAGMENTS = 10;
  private static final int ROWS_PER_FRAGMENT = 100;
  private static final int TOTAL_ROWS = NUM_FRAGMENTS * ROWS_PER_FRAGMENT;
  private static final String TABLE_NAME = "lance.default.limit_pruning_test";

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() throws Exception {
    spark =
        SparkSession.builder()
            .appName("LimitPartitionPruningTest")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.impl", "dir")
            .config("spark.sql.catalog.lance.root", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lance.default");

    // Create a dataset with NUM_FRAGMENTS fragments, ROWS_PER_FRAGMENT rows each
    spark
        .range(0, TOTAL_ROWS)
        .selectExpr("id", "id % 10 as category", "CAST(id % 2 = 0 AS boolean) as flag")
        .repartition(NUM_FRAGMENTS)
        .writeTo(TABLE_NAME)
        .create();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testLimitWithoutFilterReturnsCorrectRows() {
    Dataset<Row> result = spark.table(TABLE_NAME).limit(10);
    assertEquals(10, result.collectAsList().size());
  }

  @Test
  public void testLimitWithFilterReturnsCorrectRows() {
    Dataset<Row> result = spark.table(TABLE_NAME).filter("category = 0").limit(5);
    assertEquals(5, result.collectAsList().size());
  }

  @Test
  public void testLimitLargerThanDatasetReturnsAllRows() {
    Dataset<Row> result = spark.table(TABLE_NAME).limit(TOTAL_ROWS + 100);
    assertEquals(TOTAL_ROWS, result.collectAsList().size());
  }

  @Test
  public void testLimitWithoutFilterPrunesPartitions() {
    // LIMIT 10 on a 10-fragment dataset with 100 rows/fragment should only
    // need 1 partition (first fragment has >= 10 rows)
    Dataset<Row> limited = spark.table(TABLE_NAME).limit(10);
    int partitionCount = getInputPartitionCount(limited);

    assertTrue(
        partitionCount < NUM_FRAGMENTS,
        "Expected fewer than " + NUM_FRAGMENTS + " partitions for LIMIT 10,"
            + " but got " + partitionCount);
  }

  @Test
  public void testLimitWithFilterRetainsAllPartitions() {
    // With a filter, partition pruning is skipped — all partitions are retained
    Dataset<Row> limited = spark.table(TABLE_NAME).filter("category = 0").limit(5);
    int partitionCount = getInputPartitionCount(limited);

    assertEquals(
        NUM_FRAGMENTS,
        partitionCount,
        "With filter, all " + NUM_FRAGMENTS + " partitions should be retained");
  }

  @Test
  public void testNoLimitUsesAllPartitions() {
    Dataset<Row> all = spark.table(TABLE_NAME);
    int partitionCount = getInputPartitionCount(all);

    assertEquals(
        NUM_FRAGMENTS,
        partitionCount,
        "Without LIMIT, all " + NUM_FRAGMENTS + " partitions should be used");
  }

  /**
   * Extracts the number of input partitions from the physical plan's BatchScanExec node.
   */
  private static int getInputPartitionCount(Dataset<Row> dataset) {
    SparkPlan plan = dataset.queryExecution().executedPlan();
    scala.collection.Seq<SparkPlan> leaves = plan.collectLeaves();
    for (int i = 0; i < leaves.size(); i++) {
      SparkPlan leaf = leaves.apply(i);
      if (leaf instanceof BatchScanExec) {
        scala.collection.Seq<InputPartition> partitions =
            ((BatchScanExec) leaf).inputPartitions();
        return partitions.size();
      }
    }
    throw new AssertionError("No BatchScanExec found in plan: " + plan);
  }
}
