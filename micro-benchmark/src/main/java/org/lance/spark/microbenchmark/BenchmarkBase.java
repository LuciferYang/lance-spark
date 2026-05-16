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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Base state for all data source read benchmarks.
 * Manages SparkSession lifecycle and temporary directory for benchmark data.
 *
 * <p>Subclasses should call {@link #initSession()} at the start of their own
 * {@code @Setup(Level.Trial)} method. This class intentionally does NOT annotate
 * {@code initSession()} with {@code @Setup} to avoid double-initialization when
 * JMH invokes both parent and child setup methods.
 *
 * <p>JVM memory is controlled by the {@code @Fork} jvmArgs ({@code -Xmx4g}), not by
 * Spark's {@code spark.driver.memory} config which has no effect in local mode.
 *
 * <p>Compression: Parquet uses Snappy explicitly; Lance uses its built-in default
 * (zstd). This reflects production defaults for each format — a deliberate choice
 * to benchmark realistic out-of-the-box configurations.
 */
@State(Scope.Benchmark)  // Required: subclasses inherit this annotation for JMH state management
public class BenchmarkBase {

  protected SparkSession spark;
  protected Path tempDir;

  protected void initSession() throws IOException {
    if (spark != null && !spark.sparkContext().isStopped() && tempDir != null) {
      return;
    }
    tempDir = Files.createTempDirectory("lance-micro-benchmark-");

    // Clear any stale singleton to prevent cross-trial state contamination
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();

    spark = SparkSession.builder()
        .appName("lance-micro-benchmark")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate();

    // Warmup Spark codegen and JIT by running a trivial query
    spark.sql("SELECT sum(id) FROM range(0, 1000)").write()
        .format("noop").mode("overwrite").save();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    try {
      if (spark != null) {
        spark.stop();
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        spark = null;
      }
    } finally {
      if (tempDir != null) {
        deleteRecursive(tempDir);
        tempDir = null;
      }
    }
  }

  protected String lanceTablePath(String tableName) {
    return tempDir.resolve("lance").resolve(tableName + ".lance").toString();
  }

  protected String parquetTablePath(String tableName) {
    return tempDir.resolve("parquet").resolve(tableName).toString();
  }

  protected void writeAsLance(Dataset<Row> df, String tableName) {
    df.write()
        .format("lance")
        .mode(SaveMode.Overwrite)
        .save(lanceTablePath(tableName));
  }

  protected void writeAsParquet(Dataset<Row> df, String tableName) {
    df.write()
        .format("parquet")
        .option("compression", "snappy")
        .mode(SaveMode.Overwrite)
        .save(parquetTablePath(tableName));
  }

  protected void registerTable(String format, String tableName, String viewName) {
    Objects.requireNonNull(spark, "SparkSession not initialized -- call initSession() in @Setup");
    String path = "lance".equals(format)
        ? lanceTablePath(tableName)
        : parquetTablePath(tableName);
    spark.read().format(format).load(path).createOrReplaceTempView(viewName);
  }

  protected void executeQuery(String sql) {
    Objects.requireNonNull(spark, "SparkSession not initialized -- call initSession() in @Setup");
    spark.sql(sql).write().format("noop").mode("overwrite").save();
  }

  private static void deleteRecursive(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(path)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(p -> {
            try {
              Files.delete(p);
            } catch (IOException e) {
              System.err.println("Warning: failed to delete " + p + ": " + e.getMessage());
            }
          });
    }
  }
}
