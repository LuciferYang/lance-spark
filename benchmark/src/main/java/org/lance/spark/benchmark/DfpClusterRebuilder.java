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
package org.lance.spark.benchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Re-sorts an existing Lance table by a given column so downstream zonemap indexes produce
 * tight per-fragment min/max bounds. A DFP run against an unclustered fact table has nothing
 * to prune because every fragment's zone spans the full join-key domain.
 *
 * <p>The rebuild is done out-of-place: the source table is read, sorted, and written to
 * {@code <table>_clustered.lance}, which the caller then swaps in (e.g. via {@code mv} in the
 * orchestrating shell script). Destination-in-place rewriting would require a delete-&-rewrite
 * inside Lance and is out of scope here.
 *
 * <p>Usage:
 * <pre>
 *   spark-submit --class org.lance.spark.benchmark.DfpClusterRebuilder benchmark.jar \
 *       --src  /path/to/tpcds/lance/store_sales.lance \
 *       --dst  /path/to/tpcds/lance/store_sales_clustered.lance \
 *       --sort-by ss_sold_date_sk
 * </pre>
 */
public class DfpClusterRebuilder {

  public static void main(String[] args) {
    String src = null;
    String dst = null;
    String sortBy = null;
    int numPartitions = 0; // 0 = keep input partitioning (sortWithinPartitions only)

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--src":
          src = args[++i];
          break;
        case "--dst":
          dst = args[++i];
          break;
        case "--sort-by":
          sortBy = args[++i];
          break;
        case "--num-partitions":
          numPartitions = Integer.parseInt(args[++i]);
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (src == null || dst == null || sortBy == null) {
      System.err.println("Missing required arguments.");
      printUsage();
      System.exit(1);
    }

    SparkSession spark =
        SparkSession.builder().appName("DFP Cluster Rebuilder: " + sortBy).getOrCreate();

    try {
      System.out.println("=== DFP Cluster Rebuilder ===");
      System.out.println("Source:   " + src);
      System.out.println("Dest:     " + dst);
      System.out.println("Sort by:  " + sortBy);
      long start = System.currentTimeMillis();

      Dataset<Row> df = spark.read().format("lance").load(src);
      long rowCount = df.count();
      System.out.println("Rows:     " + rowCount);

      // When --num-partitions is given, range-partition globally on the sort column so each
      // output fragment covers a contiguous, non-overlapping slice of the key domain. This
      // maximises zonemap tightness per fragment — the precondition for DFP runtime pruning
      // to actually eliminate fragments. Without it, sortWithinPartitions only tightens
      // intra-fragment order but leaves fragment-level zone ranges overlapping if input
      // partitions already span the whole key domain.
      org.apache.spark.sql.Dataset<Row> prepared;
      if (numPartitions > 0) {
        prepared =
            df.repartitionByRange(numPartitions, df.col(sortBy)).sortWithinPartitions(sortBy);
        System.out.printf("Range-partitioning into %d fragments%n", numPartitions);
      } else {
        prepared = df.sortWithinPartitions(sortBy);
      }
      prepared.write().mode(SaveMode.ErrorIfExists).format("lance").save(dst);

      long elapsed = System.currentTimeMillis() - start;
      System.out.printf("Wrote %s in %d ms%n", dst, elapsed);
    } finally {
      spark.stop();
    }
  }

  private static void printUsage() {
    System.err.println(
        "Usage: DfpClusterRebuilder --src <path> --dst <path> --sort-by <column>");
  }
}
