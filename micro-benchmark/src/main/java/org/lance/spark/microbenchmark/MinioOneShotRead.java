/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.lance.spark.microbenchmark;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * One-shot read of Lance or Parquet from MinIO, for use with {@code mc admin trace} to count the
 * server-observed HEAD/GET requests per scan. Runs exactly one aggregation query end-to-end and
 * exits — no warmup, no JMH, no toxiproxy (direct :9000 to avoid retry noise).
 *
 * <p>Usage:
 * <pre>
 *   MinioOneShotRead &lt;lance|parquet&gt; &lt;prune|fullscan&gt; [direct|proxy] \
 *                    [cache=true|false] [profile=off|cpu|wall|alloc]
 * </pre>
 *
 * <p>When {@code profile} is not {@code off}, async-profiler is started right before the timed
 * query and stopped right after; output is written as an interactive flamegraph HTML at
 * {@code /tmp/oneshot-<format>-<query>-<profile>-<ts>.html}. Requires async-profiler's
 * {@code async-profiler.jar} on the classpath and {@code libasyncProfiler.dylib} at
 * {@code /opt/homebrew/opt/async-profiler/lib/libasyncProfiler.dylib} (override via
 * {@code -DasyncProfiler.libPath=...}).
 */
public class MinioOneShotRead {

  private static final String DIRECT_ENDPOINT = "http://localhost:9000";
  private static final String PROXY_ENDPOINT = "http://localhost:19000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";
  // dsrb/topn_data.{lance,parquet} are seeded by MinioBenchmarkDataSetup (20M rows, cols:
  // id LONG + score DOUBLE + name STRING). This is the dataset closest to the original
  // lance-gap-analysis.md setup (also 3-col, ~350MB Lance), so sum(id) here exercises
  // the same column-prune path that produced the 3198ms Lance / 1897ms Parquet gap.
  private static final String LANCE_URI = "s3://benchmark/dsrb/topn_data.lance";
  private static final String PARQUET_URI = "s3a://benchmark/dsrb/topn_data.parquet";

  public static void main(String[] args) throws Exception {
    String format = args.length >= 1 ? args[0] : "lance";
    String query = args.length >= 2 ? args[1] : "prune";
    String mode = args.length >= 3 ? args[2] : "direct";
    String cacheEnabled = args.length >= 4 ? args[3] : "true";
    String profileEvent = args.length >= 5 ? args[4] : "off";
    String queueDepth = args.length >= 6 ? args[5] : "0";
    String endpoint = "proxy".equals(mode) ? PROXY_ENDPOINT : DIRECT_ENDPOINT;

    String sql;
    if ("fullscan".equals(query)) {
      // topn_data has id LONG + score DOUBLE + name STRING — all three decode per batch.
      // The string column makes the per-batch JVM work heavier, which is where async
      // prefetch can overlap decode with I/O of the next batch.
      sql = "SELECT sum(id), sum(score), count(name) FROM benchTable";
    } else if ("prune".equals(query)) {
      sql = "SELECT sum(id) FROM benchTable";
    } else {
      throw new IllegalArgumentException("Unknown query: " + query);
    }

    SparkSession spark = SparkSession.builder()
        .appName("minio-oneshot-read-" + format + "-" + query + "-" + mode)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        // S3A for Parquet
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
        // Lance catalog-level storage options
        .config("spark.sql.catalog.lance_default",
            "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance_default.aws_access_key_id", ACCESS_KEY)
        .config("spark.sql.catalog.lance_default.aws_secret_access_key", SECRET_KEY)
        .config("spark.sql.catalog.lance_default.aws_endpoint", endpoint)
        .config("spark.sql.catalog.lance_default.aws_region", "us-east-1")
        .config("spark.sql.catalog.lance_default.aws_virtual_hosted_style_request", "false")
        .config("spark.sql.catalog.lance_default.allow_http", "true")
        .getOrCreate();

    System.out.println("=== MinioOneShotRead format=" + format + " query=" + query
        + " mode=" + mode + " cache=" + cacheEnabled + " queueDepth=" + queueDepth
        + " endpoint=" + endpoint + " ===");

    Dataset<Row> table = "lance".equals(format)
        ? spark.read()
            .format("lance")
            .option("block_size", "1048576")
            .option("batch_readahead", "16")
            .option("batch_prefetch_queue_depth", queueDepth)
            .option("dataset_cache_enabled", cacheEnabled)
            .option("aws_access_key_id", ACCESS_KEY)
            .option("aws_secret_access_key", SECRET_KEY)
            .option("aws_endpoint", endpoint)
            .option("aws_region", "us-east-1")
            .option("aws_virtual_hosted_style_request", "false")
            .option("allow_http", "true")
            .load(LANCE_URI)
        : spark.read().format("parquet").load(PARQUET_URI);

    table.createOrReplaceTempView("benchTable");

    System.out.println(">>> BEGIN MARKER: about to execute query <<<");
    System.out.println("=== PHYSICAL PLAN ===");
    spark.sql(sql).explain("formatted");
    System.out.println("=== END PHYSICAL PLAN ===");

    Object profiler = "off".equals(profileEvent) ? null : loadProfiler();
    Path jfrFile = null;
    if (profiler != null) {
      jfrFile = Paths.get("/tmp/oneshot-" + format + "-" + query + "-" + profileEvent
          + "-" + System.currentTimeMillis() + ".jfr");
      executeProfilerCommand(profiler,
          "start,event=" + profileEvent + ",interval=1000000,alluser,jfr,file=" + jfrFile);
      System.out.println(">>> PROFILER START event=" + profileEvent + " -> " + jfrFile);
    }

    long t0 = System.nanoTime();
    spark.sql(sql).write().format("noop").mode("overwrite").save();
    long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

    if (profiler != null) {
      executeProfilerCommand(profiler, "stop");
      System.out.println(">>> PROFILER STOP wrote " + jfrFile + " ("
          + (Files.exists(jfrFile) ? Files.size(jfrFile) / 1024 + " KB" : "missing") + ")");
    }
    System.out.println(">>> END MARKER: query complete, elapsed=" + elapsedMs + "ms <<<");

    spark.stop();
    System.exit(0);
  }

  /**
   * Loads {@code one.profiler.AsyncProfiler} via reflection so the class stays compilable when
   * async-profiler.jar is not on the classpath. Returns {@code null} if the class cannot be
   * found — caller should treat that as {@code profile=off}.
   */
  private static Object loadProfiler() {
    String libPath = System.getProperty("asyncProfiler.libPath",
        "/opt/homebrew/opt/async-profiler/lib/libasyncProfiler.dylib");
    try {
      Class<?> cls = Class.forName("one.profiler.AsyncProfiler");
      Method getInstance = cls.getMethod("getInstance", String.class);
      return getInstance.invoke(null, libPath);
    } catch (ReflectiveOperationException ex) {
      System.err.println("async-profiler not available (" + ex.getClass().getSimpleName()
          + ": " + ex.getMessage() + "); continuing without profiling");
      return null;
    }
  }

  private static void executeProfilerCommand(Object profiler, String command) {
    try {
      Method execute = profiler.getClass().getMethod("execute", String.class);
      Object result = execute.invoke(profiler, command);
      if (result != null && !((String) result).isEmpty()) {
        System.out.println("[asprof] " + ((String) result).trim());
      }
    } catch (ReflectiveOperationException ex) {
      throw new IllegalStateException("async-profiler command failed: " + command, ex);
    }
  }
}
