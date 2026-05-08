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

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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
 * Experimental benchmarks to measure the impact of Lance optimization knobs:
 * <ul>
 *   <li>Compression: zstd (default) vs lz4 vs none</li>
 *   <li>Batch size: 8192 (default) vs 32768 vs 65536</li>
 * </ul>
 *
 * <p>Focuses on the numeric scan scenario where Lance has the largest gap vs Parquet.
 *
 * <p>Usage:
 * <pre>
 *   mvn exec:exec -Djmh.benchmarks=".*OptimizationExperiment.*"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
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
public class OptimizationExperimentBenchmark {

  // ========================================================================
  // Experiment 1: Compression Impact on Numeric Scan
  // ========================================================================

  @State(Scope.Benchmark)
  public static class CompressionState extends BenchmarkBase {
    @Param({"zstd", "lz4", "none"})
    public String compression;

    private static final int NUM_ROWS = 15_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT cast(id as long) as id FROM range(0, " + NUM_ROWS + ")");

      DataFrameWriter<Row> writer = df.write()
          .format("lance")
          .mode(SaveMode.ErrorIfExists);

      if (!"zstd".equals(compression)) {
        writer = writer.option("id.lance.compression", compression);
      }
      writer.save(lanceTablePath("numeric_" + compression));

      spark.read().format("lance")
          .load(lanceTablePath("numeric_" + compression))
          .createOrReplaceTempView("benchTable");
    }
  }

  @Benchmark
  public void compressionNumericScan(CompressionState state) {
    state.executeQuery("SELECT sum(id) FROM benchTable");
  }

  // ========================================================================
  // Experiment 2: Batch Size Impact on Numeric Scan
  // ========================================================================

  @State(Scope.Benchmark)
  public static class BatchSizeState extends BenchmarkBase {
    @Param({"8192", "32768", "65536"})
    public int batchSize;

    private static final int NUM_ROWS = 15_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT cast(id as long) as id FROM range(0, " + NUM_ROWS + ")");

      df.write()
          .format("lance")
          .mode(SaveMode.ErrorIfExists)
          .save(lanceTablePath("numeric_batch"));

      spark.read().format("lance")
          .option("batch_size", String.valueOf(batchSize))
          .load(lanceTablePath("numeric_batch"))
          .createOrReplaceTempView("benchTable");
    }
  }

  @Benchmark
  public void batchSizeNumericScan(BatchSizeState state) {
    state.executeQuery("SELECT sum(id) FROM benchTable");
  }

  // ========================================================================
  // Experiment 3: Compression Impact on String with Nulls
  // Tests if compression difference is the root cause for string workloads.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class CompressionStringState extends BenchmarkBase {
    @Param({"zstd", "lz4", "none"})
    public String compression;

    private static final int NUM_ROWS = 10_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT "
              + "cast(id as string) as c1, "
              + "cast(id as string) as c2 "
              + "FROM range(0, " + NUM_ROWS + ")");

      DataFrameWriter<Row> writer = df.write()
          .format("lance")
          .mode(SaveMode.ErrorIfExists);

      if (!"zstd".equals(compression)) {
        writer = writer
            .option("c1.lance.compression", compression)
            .option("c2.lance.compression", compression);
      }
      writer.save(lanceTablePath("string_" + compression));

      spark.read().format("lance")
          .load(lanceTablePath("string_" + compression))
          .createOrReplaceTempView("benchTable");
    }
  }

  @Benchmark
  public void compressionStringScan(CompressionStringState state) {
    state.executeQuery("SELECT sum(length(c1)), sum(length(c2)) FROM benchTable");
  }

  // ========================================================================
  // Experiment 4: Combined Optimization (LZ4 + Large Batch)
  // Tests if compression + batch size effects compound.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class CombinedState extends BenchmarkBase {
    @Param({"baseline", "optimized"})
    public String config;

    private static final int NUM_ROWS = 15_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT cast(id as long) as id, "
              + "cast(id * 2.718 as double) as value "
              + "FROM range(0, " + NUM_ROWS + ")");

      DataFrameWriter<Row> writer = df.write()
          .format("lance")
          .mode(SaveMode.ErrorIfExists);

      if ("optimized".equals(config)) {
        writer = writer
            .option("id.lance.compression", "lz4")
            .option("value.lance.compression", "lz4");
      }
      writer.save(lanceTablePath("combined_" + config));

      int readBatchSize = "optimized".equals(config) ? 65536 : 8192;
      spark.read().format("lance")
          .option("batch_size", String.valueOf(readBatchSize))
          .load(lanceTablePath("combined_" + config))
          .createOrReplaceTempView("benchTable");
    }
  }

  @Benchmark
  public void combinedOptimization(CombinedState state) {
    state.executeQuery("SELECT sum(id), sum(value) FROM benchTable");
  }

  // ========================================================================
  // Experiment 5: Batch Readahead Impact on Numeric Scan
  // Tests whether reducing prefetch depth causes performance regression on SSD.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class BatchReadaheadState extends BenchmarkBase {
    @Param({"1", "4", "8", "16", "32"})
    public int batchReadahead;

    private static final int NUM_ROWS = 15_000_000;

    @Setup(Level.Trial)
    public void generateData() throws IOException {
      initSession();

      Dataset<Row> df = spark.sql(
          "SELECT cast(id as long) as id FROM range(0, " + NUM_ROWS + ")");

      df.write()
          .format("lance")
          .mode(SaveMode.ErrorIfExists)
          .save(lanceTablePath("numeric_readahead"));

      spark.read().format("lance")
          .option("batch_readahead", String.valueOf(batchReadahead))
          .load(lanceTablePath("numeric_readahead"))
          .createOrReplaceTempView("benchTable");
    }
  }

  @Benchmark
  public void batchReadaheadNumericScan(BatchReadaheadState state) {
    state.executeQuery("SELECT sum(id) FROM benchTable");
  }
}
