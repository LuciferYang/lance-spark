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

import org.apache.spark.sql.SparkSession;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

/**
 * MinIO-backed variant of {@link DataSourceReadBenchmark}.
 *
 * <p>All 8 scenarios from DSRB, reading from datasets pre-seeded onto MinIO by
 * {@link MinioBenchmarkDataSetup}. Reads flow through toxiproxy at {@code :19000} (100ms ± 5ms
 * downstream latency) so results reflect high-RTT object-storage behavior — the only target
 * worth tuning for in production cloud deployments.
 *
 * <p>Differences from {@link DataSourceReadBenchmark}:
 * <ul>
 *   <li>No data generation in {@code @Setup} — datasets must already exist on MinIO.</li>
 *   <li>Lance reads use the Option B path: cloud-tuned {@code block_size=1MiB},
 *       {@code batch_readahead=16}, and the driver-side {@code latestVersionId} resolve that
 *       routes driver/executor Dataset opens through {@link
 *       org.lance.spark.internal.LanceDatasetCache} with a shared pinned-version key.</li>
 *   <li>Parquet reads use {@code fadvise=random} + {@code readahead.range=64K} to prevent the
 *       default sequential prefetch window (~64MiB) from unfairly inflating wall time at 100ms
 *       RTT. Vectored IO is enabled automatically in hadoop-aws 3.4.1 + parquet-mr 1.13+.</li>
 *   <li>Shared Spark session: a single {@link MinioSession} state is reused across all
 *       scenario states so the driver JVM's {@link org.lance.spark.internal.LanceDatasetCache}
 *       accumulates warm entries across benchmark methods. Each per-scenario state pulls the
 *       live {@code SparkSession} via {@link MinioSession#get()}.</li>
 * </ul>
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>MinIO running on {@code localhost:9000}, bucket {@code benchmark}</li>
 *   <li>Toxiproxy listening on {@code localhost:19000} with the MinIO proxy + 100ms latency toxic</li>
 *   <li>Datasets seeded under {@code s3://benchmark/dsrb/...} via {@link MinioBenchmarkDataSetup}</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   mvn exec:exec -Djmh.benchmarks=".*MinioDataSourceReadBenchmark.*"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
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
public class MinioDataSourceReadBenchmark {

  static final String BUCKET = "benchmark";
  static final String DSRB_PREFIX = "dsrb";
  static final String PROXY_ENDPOINT = "http://localhost:19000";
  static final String ACCESS_KEY = "minioadmin";
  static final String SECRET_KEY = "minioadmin";

  // ========================================================================
  // Shared Spark session — one across all scenarios so the driver JVM's
  // LanceDatasetCache accumulates warm entries across benchmarks.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class MinioSession {
    private static volatile SparkSession SHARED;

    @Setup(Level.Trial)
    public void init() throws IOException {
      synchronized (MinioSession.class) {
        if (SHARED != null && !SHARED.sparkContext().isStopped()) {
          return;
        }
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        SHARED = SparkSession.builder()
            .appName("lance-minio-dsrb-benchmark")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.parquet.enableVectorizedReader", "true")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            // S3A for Parquet
            .config("spark.hadoop.fs.s3a.endpoint", PROXY_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
            .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
            // Fair-comparison tuning for Parquet over high-RTT s3a — mirrors
            // MinioLatencyBenchmark.initSession()
            .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
            .config("spark.hadoop.fs.s3a.readahead.range", "65536")
            // Lance catalog-level storage options (Option B path uses these)
            .config("spark.sql.catalog.lance_default",
                "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance_default.aws_access_key_id", ACCESS_KEY)
            .config("spark.sql.catalog.lance_default.aws_secret_access_key", SECRET_KEY)
            .config("spark.sql.catalog.lance_default.aws_endpoint", PROXY_ENDPOINT)
            .config("spark.sql.catalog.lance_default.aws_region", "us-east-1")
            .config("spark.sql.catalog.lance_default.aws_virtual_hosted_style_request", "false")
            .config("spark.sql.catalog.lance_default.allow_http", "true")
            .getOrCreate();

        // Warmup Spark codegen + JIT
        SHARED.sql("SELECT sum(id) FROM range(0, 1000)").write()
            .format("noop").mode("overwrite").save();
      }
    }

    @TearDown(Level.Trial)
    public void teardown() {
      // Intentionally keep SHARED alive across per-scenario state teardowns within a single JMH
      // fork. JMH will create multiple @State instances (one per benchmark method + @Param combo)
      // and each gets its own @TearDown(Level.Trial) call. We stop Spark only at JVM exit via
      // the shutdown hook below.
    }

    static SparkSession get() {
      if (SHARED == null) {
        throw new IllegalStateException("SHARED SparkSession not initialized");
      }
      return SHARED;
    }

    static {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        SparkSession s = SHARED;
        if (s != null) {
          try {
            s.stop();
          } catch (Exception ignore) {
            // best effort at JVM exit
          }
          SHARED = null;
        }
      }, "minio-dsrb-spark-shutdown"));
    }
  }

  // ========================================================================
  // Path + view helpers
  // ========================================================================

  private static String lanceUri(String table) {
    return "s3://" + BUCKET + "/" + DSRB_PREFIX + "/" + table + ".lance";
  }

  private static String parquetUri(String table) {
    return "s3a://" + BUCKET + "/" + DSRB_PREFIX + "/" + table + ".parquet";
  }

  private static void registerView(SparkSession spark, String format, String table, String view) {
    if ("lance".equals(format)) {
      spark.read()
          .format("lance")
          .option("block_size", "1048576")
          .option("batch_readahead", "16")
          .option("aws_access_key_id", ACCESS_KEY)
          .option("aws_secret_access_key", SECRET_KEY)
          .option("aws_endpoint", PROXY_ENDPOINT)
          .option("aws_region", "us-east-1")
          .option("aws_virtual_hosted_style_request", "false")
          .option("allow_http", "true")
          .load(lanceUri(table))
          .createOrReplaceTempView(view);
    } else {
      spark.read()
          .format("parquet")
          .load(parquetUri(table))
          .createOrReplaceTempView(view);
    }
  }

  private static void execute(SparkSession spark, String sql) {
    spark.sql(sql).write().format("noop").mode("overwrite").save();
  }

  // ========================================================================
  // Scenario 1: Numeric Column Scan
  // ========================================================================

  @State(Scope.Benchmark)
  public static class NumericScanState {
    @Param({"lance", "parquet"})
    public String format;

    @Param({"int", "long", "double"})
    public String dataType;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      registerView(MinioSession.get(), format, "numeric_" + dataType, "benchTable");
    }
  }

  @Benchmark
  public void numericScan(NumericScanState state) {
    execute(MinioSession.get(), "SELECT sum(id) FROM benchTable");
  }

  // ========================================================================
  // Scenario 2: Wide Table Column Pruning
  // ========================================================================

  @State(Scope.Benchmark)
  public static class WideTableState {
    @Param({"lance", "parquet"})
    public String format;

    @Param({"10", "50", "100"})
    public int numColumns;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      registerView(MinioSession.get(), format, "wide_table_" + numColumns, "benchTable");
    }
  }

  @Benchmark
  public void wideTableColumnPruning(WideTableState state) {
    int mid = state.numColumns / 2;
    execute(MinioSession.get(), "SELECT sum(c" + mid + ") FROM benchTable");
  }

  // ========================================================================
  // Scenario 3: Int + String Scan
  // ========================================================================

  @State(Scope.Benchmark)
  public static class IntStringScanState {
    @Param({"lance", "parquet"})
    public String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      registerView(MinioSession.get(), format, "int_string", "benchTable");
    }
  }

  @Benchmark
  public void intStringScan(IntStringScanState state) {
    execute(MinioSession.get(), "SELECT sum(c1), sum(length(c2)) FROM benchTable");
  }

  // ========================================================================
  // Scenario 4: String with Nulls
  // ========================================================================

  @State(Scope.Benchmark)
  public static class StringWithNullsState {
    @Param({"lance", "parquet"})
    public String format;

    @Param({"0.0", "0.5", "0.95"})
    public double nullFraction;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      String table = "string_nulls_" + (int) (nullFraction * 100);
      registerView(MinioSession.get(), format, table, "benchTable");
    }
  }

  @Benchmark
  public void stringWithNullsScan(StringWithNullsState state) {
    execute(MinioSession.get(),
        "SELECT sum(length(c2)) FROM benchTable WHERE c1 IS NOT NULL AND c2 IS NOT NULL");
  }

  // ========================================================================
  // Scenario 5: Predicate Filter (shared filter_data dataset)
  // ========================================================================

  @State(Scope.Benchmark)
  public static class PredicateFilterState {
    @Param({"lance", "parquet"})
    public String format;

    @Param({"low", "high"})
    public String selectivity;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      registerView(MinioSession.get(), format, "filter_data", "benchTable");
    }
  }

  @Benchmark
  public void predicateFilter(PredicateFilterState state) {
    String condition = "low".equals(state.selectivity) ? "category = 42" : "category < 500";
    execute(MinioSession.get(), "SELECT sum(value) FROM benchTable WHERE " + condition);
  }

  // ========================================================================
  // Scenario 6: Multi-Column Aggregation (GROUP BY)
  // ========================================================================

  @State(Scope.Benchmark)
  public static class AggregationState {
    @Param({"lance", "parquet"})
    public String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      registerView(MinioSession.get(), format, "agg_data", "benchTable");
    }
  }

  @Benchmark
  public void multiColumnAggregation(AggregationState state) {
    execute(MinioSession.get(),
        "SELECT dim1, sum(measure1), avg(measure2) FROM benchTable GROUP BY dim1");
  }

  // ========================================================================
  // Scenario 7: TopN Query
  // ========================================================================

  @State(Scope.Benchmark)
  public static class TopNState {
    @Param({"lance", "parquet"})
    public String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      registerView(MinioSession.get(), format, "topn_data", "benchTable");
    }
  }

  @Benchmark
  public void topNQuery(TopNState state) {
    execute(MinioSession.get(), "SELECT * FROM benchTable ORDER BY score DESC LIMIT 100");
  }

  // ========================================================================
  // Scenario 8: Range Filter
  // ========================================================================

  @State(Scope.Benchmark)
  public static class RangeFilterState {
    @Param({"lance", "parquet"})
    public String format;

    private static final long RANGE_START = (long) (10_000_000 * 0.45);
    private static final long RANGE_END = (long) (10_000_000 * 0.50);

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      registerView(MinioSession.get(), format, "range_data", "benchTable");
    }
  }

  @Benchmark
  public void rangeFilter(RangeFilterState state) {
    execute(MinioSession.get(),
        "SELECT sum(value) FROM benchTable WHERE ts BETWEEN "
            + RangeFilterState.RANGE_START + " AND " + RangeFilterState.RANGE_END);
  }

  // ========================================================================
  // Scenario 9: Async Prefetch A/B — sweep batch_prefetch_queue_depth on a
  // JVM-decode-heavy fullscan (topn_data: id LONG + score DOUBLE + name STRING,
  // 20M rows). queueDepth=0 is the baseline (synchronous Lance reader); positive
  // values enable the PrefetchingArrowReader wrapper that overlaps JVM-side
  // batch rebuild + LanceFragmentColumnarBatchScanner wrapping with Spark
  // consumption of the previous batch.
  //
  // lance-only benchmark — parquet has no analogous knob. Compare against the
  // Parquet fullscan number from earlier scenarios (e.g. IntString or WideTable)
  // if a cross-format reference is needed.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class AsyncPrefetchFullscanState {
    @Param({"0", "2", "4", "8"})
    public String queueDepth;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new MinioSession().init();
      MinioSession.get().read()
          .format("lance")
          .option("block_size", "1048576")
          .option("batch_readahead", "16")
          .option("batch_prefetch_queue_depth", queueDepth)
          .option("aws_access_key_id", ACCESS_KEY)
          .option("aws_secret_access_key", SECRET_KEY)
          .option("aws_endpoint", PROXY_ENDPOINT)
          .option("aws_region", "us-east-1")
          .option("aws_virtual_hosted_style_request", "false")
          .option("allow_http", "true")
          .load(lanceUri("topn_data"))
          .createOrReplaceTempView("benchTable");
    }
  }

  @Benchmark
  public void asyncPrefetchFullscan(AsyncPrefetchFullscanState state) {
    execute(MinioSession.get(),
        "SELECT sum(id), sum(score), count(name) FROM benchTable");
  }
}
