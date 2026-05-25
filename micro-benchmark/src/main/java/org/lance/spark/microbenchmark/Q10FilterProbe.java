/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.lance.spark.microbenchmark;

import org.apache.spark.sql.SparkSession;

/**
 * Focused probe for q10 (date_dim 73K WHERE d_date BETWEEN ...). Runs the same SQL N times in one
 * SparkSession and prints {@code iter, wall_ms, rows} per iteration. Combined with
 * {@code -Dlance.spark.probe_fixed_cost=true}, the per-fragment {@code [lance-fixed-cost]} lines
 * appear interleaved on stderr. Comparing per-iter wall to per-iter sum-of-probe lines tells us
 * whether the gap between lance (~580 ms) and parquet (~30 ms) lives inside the instrumented
 * fragment-scanner path or somewhere else (Spark Catalyst, plan refinement, multiple scans).
 *
 * <p>Usage:
 *
 * <pre>
 *   java ... -Dlance.spark.probe_fixed_cost=true \
 *     org.lance.spark.microbenchmark.Q10FilterProbe iterations=8 format=lance_bp128
 * </pre>
 */
public class Q10FilterProbe {

  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";

  private static final String SQL =
      "SELECT count(*) FROM date_dim "
          + "WHERE d_date BETWEEN DATE'1999-01-01' AND DATE'2002-12-31'";

  public static void main(String[] args) {
    int iterations = 8;
    String format = "lance_bp128";
    for (String arg : args) {
      if (arg.startsWith("iterations=")) iterations = Integer.parseInt(arg.substring(11));
      else if (arg.startsWith("format=")) format = arg.substring(7);
    }

    SparkSession spark =
        SparkSession.builder()
            .appName("q10-filter-probe")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.parquet.enableVectorizedReader", "true")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
            .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
            .config("spark.sql.catalog.lance_default",
                "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance_default.aws_access_key_id", ACCESS_KEY)
            .config("spark.sql.catalog.lance_default.aws_secret_access_key", SECRET_KEY)
            .config("spark.sql.catalog.lance_default.aws_endpoint", MINIO_ENDPOINT)
            .config("spark.sql.catalog.lance_default.aws_region", "us-east-1")
            .config("spark.sql.catalog.lance_default.aws_virtual_hosted_style_request", "false")
            .config("spark.sql.catalog.lance_default.allow_http", "true")
            .getOrCreate();

    String uri = "lance_bp128".equals(format)
        ? "s3://benchmark/tpcds-sf-100/date_dim_bp128.lance"
        : "s3a://benchmark/tpcds-sf-100/date_dim";
    if ("lance_bp128".equals(format)) {
      spark.read().format("lance")
          .option("aws_access_key_id", ACCESS_KEY)
          .option("aws_secret_access_key", SECRET_KEY)
          .option("aws_endpoint", MINIO_ENDPOINT)
          .option("aws_region", "us-east-1")
          .option("aws_virtual_hosted_style_request", "false")
          .option("allow_http", "true")
          .load(uri)
          .createOrReplaceTempView("date_dim");
    } else {
      spark.read().format("parquet").load(uri).createOrReplaceTempView("date_dim");
    }

    System.err.println("=== Q10FilterProbe format=" + format + " iterations=" + iterations + " ===");
    System.out.println(">>> iter\tformat\tpath\twall_ms");
    for (int i = 1; i <= iterations; i++) {
      // Match JMH execute(): write().format("noop").mode("overwrite").save()
      long t0 = System.nanoTime();
      spark.sql(SQL).write().format("noop").mode("overwrite").save();
      long noopMs = (System.nanoTime() - t0) / 1_000_000L;
      System.out.printf(">>> %d\t%s\tnoop\t%dms%n", i, format, noopMs);
      System.err.println("--- end iter " + i + " noop ---");

      // Comparison: .count() path
      long t1 = System.nanoTime();
      long rows = spark.sql(SQL).count();
      long countMs = (System.nanoTime() - t1) / 1_000_000L;
      System.out.printf(">>> %d\t%s\tcount\t%dms (rows=%d)%n", i, format, countMs, rows);
      System.err.println("--- end iter " + i + " count ---");
    }

    spark.stop();
  }
}
