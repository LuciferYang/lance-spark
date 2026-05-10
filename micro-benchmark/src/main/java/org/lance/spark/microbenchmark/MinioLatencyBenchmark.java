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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
 * High-latency object-storage read benchmark.
 *
 * <p>Validates the Phase 3 I/O-coalescing optimization (larger {@code block_size} default for
 * cloud paths) by reading a Lance dataset through a toxiproxy-injected 20 ms latency path. The
 * Lance scheduler coalesces small page-level reads that fall within {@code block_size} of each
 * other into a single request — with 20 ms of RTT per request, a larger block translates to
 * fewer round-trips and dramatically lower wall time.
 *
 * <p>Prerequisites (populated by {@code scripts/write_minio_data.py}):
 * <ul>
 *   <li>MinIO running on {@code localhost:9000} with bucket {@code benchmark}</li>
 *   <li>Toxiproxy listening on {@code localhost:19000} proxying to {@code localhost:9000} with a
 *       20 ms ± 5 ms downstream latency toxic</li>
 *   <li>Lance dataset {@code s3://benchmark/numeric.lance} (5M rows: id, value, name)</li>
 *   <li>Parquet file {@code s3://benchmark/numeric.parquet}</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   mvn exec:exec -Djmh.benchmarks=".*MinioLatency.*"
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
public class MinioLatencyBenchmark {

  static final String BUCKET = "benchmark";
  static final String LANCE_URI = "s3://" + BUCKET + "/numeric.lance";
  static final String PARQUET_URI = "s3a://" + BUCKET + "/numeric.parquet";

  /** Toxiproxy endpoint: injects 20ms ± 5ms downstream latency before reaching MinIO at :9000. */
  static final String PROXY_ENDPOINT = "http://localhost:19000";

  static final String ACCESS_KEY = "minioadmin";
  static final String SECRET_KEY = "minioadmin";

  /**
   * Base state: a stopped-for-reuse SparkSession configured for MinIO via toxiproxy. No local
   * data is generated — data is read directly from the bucket populated by the setup script.
   */
  @State(Scope.Benchmark)
  public static class MinioSessionBase {
    protected SparkSession spark;

    protected void initSession() throws IOException {
      if (spark != null && !spark.sparkContext().isStopped()) {
        return;
      }

      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();

      spark = SparkSession.builder()
          .appName("lance-minio-latency-benchmark")
          .master("local[*]")
          .config("spark.sql.shuffle.partitions", "8")
          .config("spark.sql.parquet.enableVectorizedReader", "true")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.adaptive.enabled", "false")
          // S3A configuration for Parquet reads through toxiproxy
          .config("spark.hadoop.fs.s3a.endpoint", PROXY_ENDPOINT)
          .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
          .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
          .config("spark.hadoop.fs.s3a.path.style.access", "true")
          .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
          .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
          .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
          // Lance catalog-level storage options (consumed by LanceSparkCatalogConfig.from()).
          // Inline .option("aws_access_key_id", ...) on spark.read.format("lance") does NOT
          // reach the native layer on the catalog path — BaseLanceNamespaceSparkCatalog uses
          // the catalog config. We set both the catalog impl and the storage options here so
          // LanceDataSource's default-catalog wiring finds credentials on initialize().
          .config("spark.sql.catalog.lance_default",
              "org.lance.spark.LanceNamespaceSparkCatalog")
          .config("spark.sql.catalog.lance_default.aws_access_key_id", ACCESS_KEY)
          .config("spark.sql.catalog.lance_default.aws_secret_access_key", SECRET_KEY)
          .config("spark.sql.catalog.lance_default.aws_endpoint", PROXY_ENDPOINT)
          .config("spark.sql.catalog.lance_default.aws_region", "us-east-1")
          .config("spark.sql.catalog.lance_default.aws_virtual_hosted_style_request", "false")
          .config("spark.sql.catalog.lance_default.allow_http", "true")
          .getOrCreate();

      // Warmup Spark codegen and JIT
      spark.sql("SELECT sum(id) FROM range(0, 1000)").write()
          .format("noop").mode("overwrite").save();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      if (spark != null) {
        spark.stop();
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        spark = null;
      }
    }

    protected void executeQuery(String sql) {
      spark.sql(sql).write().format("noop").mode("overwrite").save();
    }

    /**
     * Loads the Lance dataset from MinIO (via toxiproxy) with the given read options, and
     * registers it as a temp view {@code benchTable}.
     */
    protected Dataset<Row> loadLance(int blockSize, int batchReadahead) {
      return spark.read()
          .format("lance")
          .option("block_size", String.valueOf(blockSize))
          .option("batch_readahead", String.valueOf(batchReadahead))
          // Lance Rust object_store credentials + endpoint (via toxiproxy)
          .option("aws_access_key_id", ACCESS_KEY)
          .option("aws_secret_access_key", SECRET_KEY)
          .option("aws_endpoint", PROXY_ENDPOINT)
          .option("aws_region", "us-east-1")
          .option("aws_virtual_hosted_style_request", "false")
          .option("allow_http", "true")
          .load(LANCE_URI);
    }
  }

  // ========================================================================
  // Experiment 1: block_size impact on Lance reads under 20ms latency
  //
  // Compares the old 64KB default against the new 1MB cloud default. Each
  // request saves one 20ms round-trip, so fewer coalesced requests = big win.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class BlockSizeState extends MinioSessionBase {
    /** 64KB = old default; 1MB = new cloud default; 4MB = aggressive. */
    @Param({"65536", "1048576", "4194304"})
    public int blockSize;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      initSession();
      loadLance(blockSize, 16).createOrReplaceTempView("benchTable");
    }
  }

  @Benchmark
  public void lanceBlockSizeFullScan(BlockSizeState state) {
    state.executeQuery("SELECT sum(id), sum(value) FROM benchTable");
  }

  @Benchmark
  public void lanceBlockSizeColumnPrune(BlockSizeState state) {
    state.executeQuery("SELECT sum(id) FROM benchTable");
  }

  // ========================================================================
  // Experiment 2: batch_readahead impact under 20ms latency
  //
  // On local SSD, readahead had negligible effect. With 20ms per request,
  // higher readahead should overlap more in-flight I/O and win big.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class BatchReadaheadState extends MinioSessionBase {
    @Param({"1", "4", "16", "32", "64"})
    public int batchReadahead;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      initSession();
      loadLance(1_048_576, batchReadahead).createOrReplaceTempView("benchTable");
    }
  }

  @Benchmark
  public void lanceBatchReadaheadFullScan(BatchReadaheadState state) {
    state.executeQuery("SELECT sum(id), sum(value) FROM benchTable");
  }

  @Benchmark
  public void lanceBatchReadaheadColumnPrune(BatchReadaheadState state) {
    state.executeQuery("SELECT sum(id) FROM benchTable");
  }

  // ========================================================================
  // Experiment 3: Lance vs Parquet baseline under 20ms latency
  //
  // Uses the new 1MB default for Lance; compares against Parquet reading
  // through the same toxiproxy endpoint.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class FormatComparisonState extends MinioSessionBase {
    @Param({"lance", "parquet"})
    public String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      initSession();
      if ("lance".equals(format)) {
        loadLance(1_048_576, 16).createOrReplaceTempView("benchTable");
      } else {
        spark.read().format("parquet").load(PARQUET_URI).createOrReplaceTempView("benchTable");
      }
    }
  }

  @Benchmark
  public void formatFullScan(FormatComparisonState state) {
    state.executeQuery("SELECT sum(id), sum(value) FROM benchTable");
  }

  @Benchmark
  public void formatColumnPrune(FormatComparisonState state) {
    state.executeQuery("SELECT sum(id) FROM benchTable");
  }
}
