/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.lance.spark.microbenchmark;

import org.apache.spark.sql.SparkSession;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Multi-iteration single-process probe to verify whether the JMH "outliers" (q8/q10/q18 — filter +
 * COUNT(*) on small/medium tables) reflect real lance overhead or are a JMH measurement artifact.
 *
 * <p>For each named query, runs the JMH-equivalent {@code spark.sql(sql).write.format("noop").save()}
 * call 50 times in one SparkSession and prints per-iteration wall_ms. Compare iter 5+ steady-state to
 * the JMH avgt result.
 */
public class FilterPathProbe {

  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";

  private static final Map<String, QSpec> QUERIES = new LinkedHashMap<>();

  static {
    QUERIES.put("q8",
        new QSpec("customer",
            "SELECT count(*) FROM customer WHERE c_first_name = 'James'"));
    QUERIES.put("q10",
        new QSpec("date_dim",
            "SELECT count(*) FROM date_dim "
                + "WHERE d_date BETWEEN DATE'1999-01-01' AND DATE'2002-12-31'"));
    QUERIES.put("q18",
        new QSpec("synth/temporal",
            "SELECT count(*) FROM synth_temporal "
                + "WHERE v_timestamp BETWEEN TIMESTAMP'2018-01-01 00:00:00' "
                + "AND TIMESTAMP'2020-01-01 00:00:00'"));
  }

  private static final class QSpec {
    final String tableSlug;
    final String sql;

    QSpec(String tableSlug, String sql) {
      this.tableSlug = tableSlug;
      this.sql = sql;
    }
  }

  public static void main(String[] args) {
    int iterations = 30;
    String format = "lance_bp128";
    String only = null;
    for (String arg : args) {
      if (arg.startsWith("iterations=")) iterations = Integer.parseInt(arg.substring(11));
      else if (arg.startsWith("format=")) format = arg.substring(7);
      else if (arg.startsWith("only=")) only = arg.substring(5);
    }

    SparkSession spark =
        SparkSession.builder()
            .appName("filter-path-probe")
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

    System.out.println(">>> q\tformat\titer\twall_ms");
    for (Map.Entry<String, QSpec> e : QUERIES.entrySet()) {
      String tag = e.getKey();
      QSpec q = e.getValue();
      if (only != null && !only.equals(tag)) continue;

      // Register the view from the right URI for this format.
      String viewName;
      String uri;
      if (q.tableSlug.startsWith("synth/")) {
        viewName = "synth_" + q.tableSlug.substring("synth/".length());
        uri = "lance_bp128".equals(format)
            ? "s3://benchmark/synth/" + q.tableSlug.substring("synth/".length()) + "_bp128.lance"
            : "s3a://benchmark/synth/" + q.tableSlug.substring("synth/".length());
      } else {
        viewName = q.tableSlug;
        uri = "lance_bp128".equals(format)
            ? "s3://benchmark/tpcds-sf-100/" + q.tableSlug + "_bp128.lance"
            : "s3a://benchmark/tpcds-sf-100/" + q.tableSlug;
      }

      if ("lance_bp128".equals(format)) {
        spark.read().format("lance")
            .option("aws_access_key_id", ACCESS_KEY)
            .option("aws_secret_access_key", SECRET_KEY)
            .option("aws_endpoint", MINIO_ENDPOINT)
            .option("aws_region", "us-east-1")
            .option("aws_virtual_hosted_style_request", "false")
            .option("allow_http", "true")
            .load(uri)
            .createOrReplaceTempView(viewName);
      } else {
        spark.read().format("parquet").load(uri).createOrReplaceTempView(viewName);
      }

      for (int i = 1; i <= iterations; i++) {
        long t0 = System.nanoTime();
        spark.sql(q.sql).write().format("noop").mode("overwrite").save();
        long noopMs = (System.nanoTime() - t0) / 1_000_000L;

        long t1 = System.nanoTime();
        long rows = spark.sql(q.sql).count();
        long countMs = (System.nanoTime() - t1) / 1_000_000L;

        long t2 = System.nanoTime();
        long collectRows = spark.sql(q.sql).collectAsList().size();
        long collectMs = (System.nanoTime() - t2) / 1_000_000L;

        System.out.printf(">>> %s\t%s\t%d\tnoop=%dms count=%dms collect=%dms (rows=%d/%d)%n",
            tag, format, i, noopMs, countMs, collectMs, rows, collectRows);
      }
    }

    spark.stop();
  }
}
