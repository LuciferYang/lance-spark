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

import java.util.Map;
import java.util.Optional;

/**
 * Canonical Hive-style partition column per TPC-DS fact table.
 *
 * <p>Mirrors the convention used by Databricks {@code spark-sql-perf}
 * (see {@code TPCDSTables.scala}) so partitioned Parquet output is directly
 * comparable across benchmarking studies. Only the seven fact tables are
 * partitioned, each by its date_sk; all dimensions stay flat.
 */
final class TpcdsPartitioning {

  private static final Map<String, String> PARTITION_COL =
      Map.of(
          "catalog_sales", "cs_sold_date_sk",
          "catalog_returns", "cr_returned_date_sk",
          "inventory", "inv_date_sk",
          "store_sales", "ss_sold_date_sk",
          "store_returns", "sr_returned_date_sk",
          "web_sales", "ws_sold_date_sk",
          "web_returns", "wr_returned_date_sk");

  private TpcdsPartitioning() {}

  /** Returns the partition column for a fact table, or empty for dimensions. */
  static Optional<String> partitionColumn(String table) {
    return Optional.ofNullable(PARTITION_COL.get(table));
  }
}
