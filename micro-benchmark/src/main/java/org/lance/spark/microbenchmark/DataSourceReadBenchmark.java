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
package org.lance.spark.microbenchmark;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH benchmarks comparing Lance vs Parquet data source read performance
 * for OLAP-oriented workloads.
 *
 * <p>Benchmarks follow the design of Spark's DataSourceReadBenchmark but focus
 * on scenarios most relevant to analytical queries: full scans, column pruning,
 * predicate pushdown, aggregations, and TopN.
 *
 * <p>Usage:
 * <pre>
 *   # Build (requires lance-spark-bundle in local maven repo):
 *   cd micro-benchmark && mvn clean package
 *
 *   # Run all benchmarks via Maven:
 *   mvn exec:exec
 *
 *   # Run specific benchmark:
 *   mvn exec:exec -Djmh.benchmarks=".*numericScan.*"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
// @Fork(1) for development speed; use -f 3 via direct java invocation for publishable results.
// Keep JVM args in sync with exec-maven-plugin arguments in pom.xml
@Fork(value = 1, jvmArgs = {
    "-Xms4g",
    "-Xmx4g",
    "-XX:+UseG1GC",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "-Djdk.reflect.useDirectMethodHandle=false",
    "-Dio.netty.tryReflectionSetAccessible=true"
})
public class DataSourceReadBenchmark {

  // ========================================================================
  // Scenario 1: Numeric Column Scan
  // Tests raw columnar scan + aggregation throughput for primitive types.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class NumericScanState extends BenchmarkBase {
    @Param({"lance", "parquet"})
    public String format;

    @Param({"int", "long", "double"})
    public String dataType;

    private static final int NUM_ROWS = 15_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      String castExpr;
      switch (dataType) {
        case "int":
          castExpr = "cast(id as int)";
          break;
        case "long":
          castExpr = "cast(id as long)";
          break;
        case "double":
          castExpr = "cast(id as double)";
          break;
        default:
          throw new IllegalArgumentException("Unknown dataType: " + dataType);
      }

      Dataset<Row> df = spark.sql(
          "SELECT " + castExpr + " as id FROM range(0, " + NUM_ROWS + ")");

      if ("lance".equals(format)) {
        writeAsLance(df, "numeric_scan");
      } else {
        writeAsParquet(df, "numeric_scan");
      }
      registerTable(format, "numeric_scan", "benchTable");
    }
  }

  @Benchmark
  public void numericScan(NumericScanState state) {
    state.executeQuery("SELECT sum(id) FROM benchTable");
  }

  // ========================================================================
  // Scenario 2: Wide Table Column Pruning
  // Tests column pruning efficiency — reading 1 column from a wide table.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class WideTableState extends BenchmarkBase {
    @Param({"lance", "parquet"})
    public String format;

    @Param({"10", "50", "100"})
    public int numColumns;

    private static final int NUM_ROWS = 1_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      String selectExpr = IntStream.rangeClosed(1, numColumns)
          .mapToObj(i -> "cast((id + " + i + ") as int) as c" + i)
          .collect(Collectors.joining(", "));

      Dataset<Row> df = spark.sql(
          "SELECT " + selectExpr + " FROM range(0, " + NUM_ROWS + ")");

      String tableName = "wide_table_" + numColumns;
      if ("lance".equals(format)) {
        writeAsLance(df, tableName);
      } else {
        writeAsParquet(df, tableName);
      }
      registerTable(format, tableName, "benchTable");
    }
  }

  @Benchmark
  public void wideTableColumnPruning(WideTableState state) {
    int mid = state.numColumns / 2;
    state.executeQuery("SELECT sum(c" + mid + ") FROM benchTable");
  }

  // ========================================================================
  // Scenario 3: Int + String Scan
  // Tests mixed-type scan performance.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class IntStringScanState extends BenchmarkBase {
    @Param({"lance", "parquet"})
    public String format;

    private static final int NUM_ROWS = 10_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT cast(id as int) as c1, "
              + "cast(id as string) as c2 "
              + "FROM range(0, " + NUM_ROWS + ")");

      if ("lance".equals(format)) {
        writeAsLance(df, "int_string");
      } else {
        writeAsParquet(df, "int_string");
      }
      registerTable(format, "int_string", "benchTable");
    }
  }

  @Benchmark
  public void intStringScan(IntStringScanState state) {
    state.executeQuery("SELECT sum(c1), sum(length(c2)) FROM benchTable");
  }

  // ========================================================================
  // Scenario 4: String with Nulls
  // Tests null handling and filter pushdown efficiency.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class StringWithNullsState extends BenchmarkBase {
    @Param({"lance", "parquet"})
    public String format;

    @Param({"0.0", "0.5", "0.95"})
    public double nullFraction;

    private static final int NUM_ROWS = 10_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT "
              + "IF(rand(1) < " + nullFraction + ", NULL, cast(id as string)) as c1, "
              + "IF(rand(2) < " + nullFraction + ", NULL, cast(id as string)) as c2 "
              + "FROM range(0, " + NUM_ROWS + ")");

      String tableName = "string_nulls_" + (int) (nullFraction * 100);
      if ("lance".equals(format)) {
        writeAsLance(df, tableName);
      } else {
        writeAsParquet(df, tableName);
      }
      registerTable(format, tableName, "benchTable");
    }
  }

  @Benchmark
  public void stringWithNullsScan(StringWithNullsState state) {
    state.executeQuery(
        "SELECT sum(length(c2)) FROM benchTable WHERE c1 IS NOT NULL AND c2 IS NOT NULL");
  }

  // ========================================================================
  // Scenario 5: Predicate Filter with Configurable Selectivity
  // Tests filter pushdown at various selectivity levels.
  // Data is shuffled to avoid sorted-data bias in row group statistics.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class PredicateFilterState extends BenchmarkBase {
    @Param({"lance", "parquet"})
    public String format;

    @Param({"low", "high"})
    public String selectivity;

    private static final int NUM_ROWS = 10_000_000;
    private static final int NUM_CATEGORIES = 1000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT "
              + "cast(id as int) as id, "
              + "cast(id % " + NUM_CATEGORIES + " as int) as category, "
              + "cast(id * 0.31415 as double) as value "
              + "FROM range(0, " + NUM_ROWS + ") "
              + "ORDER BY rand(7)");

      String tableName = "filter_data_" + selectivity;
      if ("lance".equals(format)) {
        writeAsLance(df, tableName);
      } else {
        writeAsParquet(df, tableName);
      }
      registerTable(format, tableName, "benchTable");
    }
  }

  @Benchmark
  public void predicateFilter(PredicateFilterState state) {
    String condition = "low".equals(state.selectivity) ? "category = 42" : "category < 500";
    state.executeQuery("SELECT sum(value) FROM benchTable WHERE " + condition);
  }

  // ========================================================================
  // Scenario 6: Multi-Column Aggregation (GROUP BY)
  // Tests scan + aggregation for OLAP GROUP BY queries.
  // Uses larger dataset to ensure scan dominates over aggregation overhead.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class AggregationState extends BenchmarkBase {
    @Param({"lance", "parquet"})
    public String format;

    private static final int NUM_ROWS = 20_000_000;
    private static final int DIM1_CARDINALITY = 100;
    private static final int DIM2_CARDINALITY = 50;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT "
              + "cast(id % " + DIM1_CARDINALITY + " as int) as dim1, "
              + "cast(id % " + DIM2_CARDINALITY + " as int) as dim2, "
              + "concat('str_', cast(id % 200 as string)) as dim3, "
              + "cast(id * 0.7 as double) as measure1, "
              + "cast(id * 1.3 as double) as measure2 "
              + "FROM range(0, " + NUM_ROWS + ")");

      if ("lance".equals(format)) {
        writeAsLance(df, "agg_data");
      } else {
        writeAsParquet(df, "agg_data");
      }
      registerTable(format, "agg_data", "benchTable");
    }
  }

  @Benchmark
  public void multiColumnAggregation(AggregationState state) {
    state.executeQuery(
        "SELECT dim1, sum(measure1), avg(measure2) FROM benchTable GROUP BY dim1");
  }

  // ========================================================================
  // Scenario 7: TopN Query (ORDER BY ... LIMIT)
  // Tests Lance's TopN push-down vs Parquet full-scan-then-sort.
  // Uses larger dataset so sort cost is non-trivial.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class TopNState extends BenchmarkBase {
    @Param({"lance", "parquet"})
    public String format;

    private static final int NUM_ROWS = 20_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT "
              + "id, "
              + "cast(id * 0.12345 + rand(42) * 1000 as double) as score, "
              + "concat('name_', cast(id % 100000 as string)) as name "
              + "FROM range(0, " + NUM_ROWS + ")");

      if ("lance".equals(format)) {
        writeAsLance(df, "topn_data");
      } else {
        writeAsParquet(df, "topn_data");
      }
      registerTable(format, "topn_data", "benchTable");
    }
  }

  @Benchmark
  public void topNQuery(TopNState state) {
    state.executeQuery("SELECT * FROM benchTable ORDER BY score DESC LIMIT 100");
  }

  // ========================================================================
  // Scenario 8: Range Filter on Sorted Column
  // Tests range scan efficiency on monotonically increasing data (~5% range).
  // ========================================================================

  @State(Scope.Benchmark)
  public static class RangeFilterState extends BenchmarkBase {
    @Param({"lance", "parquet"})
    public String format;

    private static final int NUM_ROWS = 10_000_000;
    private static final long RANGE_START = (long) (NUM_ROWS * 0.45);
    private static final long RANGE_END = (long) (NUM_ROWS * 0.50);

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT "
              + "id as ts, "
              + "cast(id * 2.718 as double) as value "
              + "FROM range(0, " + NUM_ROWS + ")");

      if ("lance".equals(format)) {
        writeAsLance(df, "range_data");
      } else {
        writeAsParquet(df, "range_data");
      }
      registerTable(format, "range_data", "benchTable");
    }
  }

  @Benchmark
  public void rangeFilter(RangeFilterState state) {
    state.executeQuery(
        "SELECT sum(value) FROM benchTable WHERE ts BETWEEN "
            + RangeFilterState.RANGE_START + " AND " + RangeFilterState.RANGE_END);
  }
}
