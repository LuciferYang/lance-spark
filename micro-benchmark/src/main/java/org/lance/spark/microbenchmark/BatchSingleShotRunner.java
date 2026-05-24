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
 * Single-shot validator for the type-coverage benchmark suite (q5-q12). Wires a Spark
 * session against MinIO with the same view registration as
 * {@link MinioTpcdsBenchmark.TypeCovState}, then runs each named query once per format
 * and prints {@code (q_tag, format, rows_or_msg, wall_ms)} rows.
 *
 * <p>Purpose: smoke-test that all 8 SQL queries actually execute against the MinIO data
 * before paying the cost of a full JMH run. Catches missing views, schema mismatches,
 * and column-name typos in O(minutes) rather than O(hours).
 *
 * <p>Usage:
 * <pre>
 *   java ... org.lance.spark.microbenchmark.BatchSingleShotRunner
 *   java ... org.lance.spark.microbenchmark.BatchSingleShotRunner only=q5,q9
 *   java ... org.lance.spark.microbenchmark.BatchSingleShotRunner formats=lance_bp128
 * </pre>
 */
public class BatchSingleShotRunner {

  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";

  /** Insertion order = run order. */
  private static final Map<String, String> QUERIES = new LinkedHashMap<>();
  static {
    QUERIES.put("q1", "SELECT sum(ss_quantity) FROM store_sales");
    QUERIES.put("q4", "SELECT sum(ss_wholesale_cost), sum(ss_list_price), sum(ss_sales_price), "
        + "sum(ss_ext_sales_price), sum(ss_net_paid), sum(ss_net_profit) FROM store_sales");
    QUERIES.put("q5", "SELECT count(distinct c_customer_sk) FROM customer");
    QUERIES.put("q6", "SELECT sum(sr_fee), sum(sr_return_tax) FROM store_returns");
    QUERIES.put("q7", "SELECT count(distinct i_product_name) FROM item");
    QUERIES.put("q8", "SELECT count(*) FROM customer WHERE c_first_name = 'James'");
    QUERIES.put("q9", "SELECT ca_state, count(*) FROM customer_address GROUP BY ca_state");
    QUERIES.put("q10", "SELECT count(*) FROM date_dim "
        + "WHERE d_date BETWEEN DATE'1999-01-01' AND DATE'2002-12-31'");
    QUERIES.put("q11", "SELECT d_year, count(*) FROM date_dim GROUP BY d_year");
    QUERIES.put("q12", "SELECT d_year, sum(ss_quantity) "
        + "FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk "
        + "GROUP BY d_year");
    QUERIES.put("q13", "SELECT sum(v_int64) FROM synth_numeric");
    QUERIES.put("q14", "SELECT sum(v_double) FROM synth_numeric");
    QUERIES.put("q15", "SELECT sum(v_float) FROM synth_numeric");
    QUERIES.put("q16", "SELECT sum(v_decimal_18_2) FROM synth_numeric");
    QUERIES.put("q17", "SELECT sum(v_decimal_38_18) FROM synth_numeric");
    QUERIES.put("q18", "SELECT count(*) FROM synth_temporal "
        + "WHERE v_timestamp BETWEEN TIMESTAMP'2018-01-01 00:00:00' "
        + "AND TIMESTAMP'2020-01-01 00:00:00'");
    QUERIES.put("q19", "SELECT sum(CASE WHEN v_boolean THEN 1 ELSE 0 END) FROM synth_numeric");
  }

  public static void main(String[] args) {
    java.util.Set<String> only = null;
    String[] formats = {"lance_bp128", "parquet"};
    for (String arg : args) {
      if (arg.startsWith("only=")) {
        only = new java.util.HashSet<>();
        for (String t : arg.substring("only=".length()).split(",")) only.add(t.trim());
      } else if (arg.startsWith("formats=")) {
        formats = arg.substring("formats=".length()).split(",");
      }
    }

    SparkSession spark = SparkSession.builder()
        .appName("type-cov-single-shot")
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

    System.out.println("\n>>> q_tag\tformat\trows\twall_ms");
    int failures = 0;

    for (String format : formats) {
      registerAllViews(spark, format);
      for (Map.Entry<String, String> e : QUERIES.entrySet()) {
        String tag = e.getKey();
        if (only != null && !only.contains(tag)) continue;
        String sql = e.getValue();
        long t0 = System.nanoTime();
        try {
          long rows = spark.sql(sql).count();
          long wallMs = (System.nanoTime() - t0) / 1_000_000L;
          System.out.printf(">>> %-4s\t%-12s\t%-10d\t%dms%n", tag, format, rows, wallMs);
        } catch (Exception ex) {
          long wallMs = (System.nanoTime() - t0) / 1_000_000L;
          System.out.printf(">>> %-4s\t%-12s\tERR        \t%dms\t%s: %s%n",
              tag, format, wallMs, ex.getClass().getSimpleName(), ex.getMessage());
          failures++;
        }
      }
    }

    spark.stop();
    System.exit(failures == 0 ? 0 : 1);
  }

  private static void registerAllViews(SparkSession spark, String format) {
    boolean isLance = "lance_bp128".equals(format);
    String storeSalesUri = isLance
        ? "s3://benchmark/tpcds-sf-100/store_sales_bp128_dispatch.lance"
        : "s3a://benchmark/tpcds-sf-100/store_sales";
    registerView(spark, format, storeSalesUri, "store_sales");

    for (String table : new String[] {
        "customer", "customer_address", "item", "date_dim", "store_returns"}) {
      String uri = isLance
          ? "s3://benchmark/tpcds-sf-100/" + table + "_bp128.lance"
          : "s3a://benchmark/tpcds-sf-100/" + table;
      registerView(spark, format, uri, table);
    }

    // Synthetic tables (batch 2)
    for (String table : new String[] {"numeric", "temporal"}) {
      String uri = isLance
          ? "s3://benchmark/synth/" + table + "_bp128.lance"
          : "s3a://benchmark/synth/" + table;
      registerView(spark, format, uri, "synth_" + table);
    }
  }

  private static void registerView(SparkSession spark, String format, String uri, String view) {
    try {
      if ("lance_bp128".equals(format)) {
        spark.read()
            .format("lance")
            .option("aws_access_key_id", ACCESS_KEY)
            .option("aws_secret_access_key", SECRET_KEY)
            .option("aws_endpoint", MINIO_ENDPOINT)
            .option("aws_region", "us-east-1")
            .option("aws_virtual_hosted_style_request", "false")
            .option("allow_http", "true")
            .load(uri)
            .createOrReplaceTempView(view);
      } else {
        spark.read()
            .format("parquet")
            .load(uri)
            .createOrReplaceTempView(view);
      }
    } catch (Exception ex) {
      System.err.println("WARN: failed to register " + view + " (" + format + ") from " + uri
          + " — queries that depend on it will fail. " + ex.getClass().getSimpleName()
          + ": " + ex.getMessage());
    }
  }
}
