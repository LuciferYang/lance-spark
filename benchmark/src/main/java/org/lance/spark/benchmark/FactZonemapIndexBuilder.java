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

import org.lance.Dataset;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates {@link IndexType#ZONEMAP} indexes on TPC-DS fact tables via the direct lance-core Java
 * API. Bypasses the {@code ALTER TABLE ... CREATE INDEX USING zonemap} SQL path because the
 * distributed {@code FragmentBasedIndexJob} cannot train zonemap indexes (Lance requires global,
 * non-fragmented training). The Java API accepts {@code fragmentIds=null} which triggers global
 * training internally.
 *
 * <p>Run after dimension btree indexes are built via {@link TpcdsIndexBuilder} but before
 * benchmarking — the Phase 1 column-stats path only loads stats for ZONEMAP-typed indexes.
 *
 * <pre>
 *   spark-submit --class org.lance.spark.benchmark.FactZonemapIndexBuilder \
 *     benchmark.jar \
 *     --data-dir /path/to/tpcds/data
 * </pre>
 */
public class FactZonemapIndexBuilder {

  /** Fact tables and the join-key columns to index. Mirrors {@code index/store_sales.sql} etc. */
  private static final Map<String, List<String>> FACT_INDEXES = new LinkedHashMap<>();

  static {
    FACT_INDEXES.put(
        "store_sales",
        Arrays.asList(
            "ss_sold_date_sk",
            "ss_item_sk",
            "ss_customer_sk",
            "ss_store_sk",
            "ss_promo_sk"));
    FACT_INDEXES.put(
        "web_sales",
        Arrays.asList("ws_sold_date_sk", "ws_item_sk", "ws_bill_customer_sk"));
    FACT_INDEXES.put(
        "catalog_sales",
        Arrays.asList("cs_sold_date_sk", "cs_item_sk", "cs_bill_customer_sk"));
  }

  public static void main(String[] args) {
    String dataDir = null;
    for (int i = 0; i < args.length; i++) {
      if ("--data-dir".equals(args[i])) {
        dataDir = args[++i];
      }
    }
    if (dataDir == null) {
      System.err.println("Usage: FactZonemapIndexBuilder --data-dir <path>");
      System.exit(1);
    }

    String lanceRoot = dataDir + "/lance";
    System.out.println("=== Fact Zonemap Index Builder ===");
    System.out.println("Lance root: " + lanceRoot);

    int total = FACT_INDEXES.values().stream().mapToInt(List::size).sum();
    int succeeded = 0;
    int skipped = 0;
    int failed = 0;
    int idx = 0;

    for (Map.Entry<String, List<String>> entry : FACT_INDEXES.entrySet()) {
      String table = entry.getKey();
      String tablePath = lanceRoot + "/" + table + ".lance";
      try (Dataset dataset = Dataset.open(tablePath)) {
        java.util.List<String> existing = dataset.listIndexes();
        for (String column : entry.getValue()) {
          idx++;
          String indexName = "idx_" + column;
          System.out.printf("[%d/%d] %s.%s ", idx, total, table, indexName);
          if (existing.contains(indexName)) {
            System.out.println("SKIP (exists)");
            skipped++;
            continue;
          }
          long start = System.currentTimeMillis();
          try {
            IndexParams params =
                IndexParams.builder()
                    .setScalarIndexParams(ScalarIndexParams.create("zonemap"))
                    .build();
            IndexOptions opts =
                IndexOptions.builder(
                        Collections.singletonList(column), IndexType.ZONEMAP, params)
                    .withIndexName(indexName)
                    .replace(false)
                    .train(true)
                    .build();
            dataset.createIndex(opts);
            long elapsed = System.currentTimeMillis() - start;
            System.out.printf("OK (%dms)%n", elapsed);
            succeeded++;
          } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - start;
            System.out.printf("FAILED (%dms) — %s%n", elapsed, e.getMessage());
            failed++;
          }
        }
      } catch (Exception e) {
        System.err.println("Failed to open " + tablePath + ": " + e.getMessage());
        failed += entry.getValue().size();
        idx += entry.getValue().size();
      }
    }

    System.out.println();
    System.out.println("=== Done ===");
    System.out.printf("Succeeded: %d, Skipped: %d, Failed: %d%n", succeeded, skipped, failed);
  }
}
