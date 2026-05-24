/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.lance.spark.microbenchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Bulk writer for the type-coverage A/B matrix. Reads the locally-staged TPC-DS lance datasets
 * at {@code file:///Users/yangjie01/Tools/tpcds-sf-100/lance/&lt;table&gt;.lance} and writes each
 * table out to MinIO in two formats so the benchmark has both A/B endpoints:
 * <ul>
 *   <li>{@code s3a://benchmark/tpcds-sf-100/&lt;table&gt;/} — Parquet, written by Spark/Hadoop
 *       (matches the existing {@code store_sales/} layout)</li>
 *   <li>{@code s3://benchmark/tpcds-sf-100/&lt;table&gt;_bp128.lance} — Lance V2 with the current
 *       writer (bp128 dispatch automatically engaged for fixed-width columns by PR#6858)</li>
 * </ul>
 *
 * <p>Tables (chosen to cover types missing from store_sales):
 * <ul>
 *   <li>{@code customer} — int32 keys + ~14 varchar (names, addresses)</li>
 *   <li>{@code customer_address} — char(2) state + varchar address fields</li>
 *   <li>{@code item} — int32 keys + ~16 varchar + decimal current_price</li>
 *   <li>{@code date_dim} — int32 keys + real {@code date} type + varchar day/month names</li>
 *   <li>{@code store_returns} — int32 keys + 10 decimal(7,2) amounts (~3 GiB local)</li>
 * </ul>
 *
 * <p>Run via:
 * <pre>
 *   ./mvnw -pl micro-benchmark -am exec:java \
 *     -Dexec.mainClass=org.lance.spark.microbenchmark.MultiTableSetup \
 *     -DskipTests -Dcheckstyle.skip=true -Dmaven.javadoc.skip=true
 * </pre>
 *
 * <p>CLI args:
 * <ul>
 *   <li>{@code overwrite} — switch SaveMode from ErrorIfExists to Overwrite for both formats</li>
 *   <li>{@code only=customer,item} — only process the comma-separated subset</li>
 *   <li>{@code skipParquet} — skip the Parquet writes (only do lance)</li>
 *   <li>{@code skipLance} — skip the lance writes (only do Parquet)</li>
 * </ul>
 */
public class MultiTableSetup {

  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";
  private static final String LOCAL_LANCE_BASE = "file:///Users/yangjie01/Tools/tpcds-sf-100/lance/";
  private static final String PARQUET_BASE = "s3a://benchmark/tpcds-sf-100/";
  private static final String LANCE_BASE = "s3://benchmark/tpcds-sf-100/";

  /** {table_name, lance_target_basename (without _bp128.lance suffix)} */
  private static final String[][] TABLES = new String[][] {
      {"customer", "customer_bp128"},
      {"customer_address", "customer_address_bp128"},
      {"item", "item_bp128"},
      {"date_dim", "date_dim_bp128"},
      {"store_returns", "store_returns_bp128"},
  };

  public static void main(String[] args) {
    boolean overwrite = false;
    boolean skipParquet = false;
    boolean skipLance = false;
    java.util.Set<String> only = null;
    for (String arg : args) {
      if ("overwrite".equalsIgnoreCase(arg)) {
        overwrite = true;
      } else if ("skipParquet".equalsIgnoreCase(arg)) {
        skipParquet = true;
      } else if ("skipLance".equalsIgnoreCase(arg)) {
        skipLance = true;
      } else if (arg.startsWith("only=")) {
        only = new java.util.HashSet<>();
        for (String t : arg.substring("only=".length()).split(",")) {
          only.add(t.trim());
        }
      }
    }
    SaveMode saveMode = overwrite ? SaveMode.Overwrite : SaveMode.ErrorIfExists;

    MinioTpcdsBenchmark.printJarFingerprint();

    SparkSession spark = SparkSession.builder()
        .appName("multi-table-bp128-setup")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        // Lance V2 write path requires storage options on the catalog namespace.
        .config("spark.sql.catalog.lance_default",
            "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance_default.aws_access_key_id", ACCESS_KEY)
        .config("spark.sql.catalog.lance_default.aws_secret_access_key", SECRET_KEY)
        .config("spark.sql.catalog.lance_default.aws_endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.lance_default.aws_region", "us-east-1")
        .config("spark.sql.catalog.lance_default.aws_virtual_hosted_style_request", "false")
        .config("spark.sql.catalog.lance_default.allow_http", "true")
        .getOrCreate();

    List<String> summary = new ArrayList<>();
    int successes = 0;
    int failures = 0;

    for (String[] entry : TABLES) {
      String tag = entry[0];
      if (only != null && !only.contains(tag)) {
        summary.add(String.format("SKIP %-20s (filtered by only=)", tag));
        continue;
      }

      String sourceLance = LOCAL_LANCE_BASE + tag + ".lance";
      String targetParquet = PARQUET_BASE + tag + "/";
      String targetLance = LANCE_BASE + entry[1] + ".lance";

      System.err.println("\n=== [" + tag + "] reading local lance: " + sourceLance + " ===");
      try {
        Dataset<Row> df = spark.read().format("lance").load(sourceLance);
        long rows = df.count();
        System.err.println("=== [" + tag + "] source rows = " + rows + " ===");

        long parquetSec = -1;
        if (!skipParquet) {
          System.err.println("=== [" + tag + "] writing parquet: " + targetParquet
              + " (mode=" + saveMode + ") ===");
          long t0 = System.nanoTime();
          df.write()
              .format("parquet")
              .mode(saveMode)
              .save(targetParquet);
          parquetSec = (System.nanoTime() - t0) / 1_000_000_000L;
          System.err.println("=== [" + tag + "] parquet done, elapsed=" + parquetSec + "s ===");
        }

        long lanceSec = -1;
        if (!skipLance) {
          System.err.println("=== [" + tag + "] writing lance: " + targetLance
              + " (mode=" + saveMode + ") ===");
          long t0 = System.nanoTime();
          df.write()
              .format("lance")
              .option("aws_access_key_id", ACCESS_KEY)
              .option("aws_secret_access_key", SECRET_KEY)
              .option("aws_endpoint", MINIO_ENDPOINT)
              .option("aws_virtual_hosted_style_request", "false")
              .option("aws_region", "us-east-1")
              .option("allow_http", "true")
              .mode(saveMode)
              .save(targetLance);
          lanceSec = (System.nanoTime() - t0) / 1_000_000_000L;
          System.err.println("=== [" + tag + "] lance done, elapsed=" + lanceSec + "s ===");
        }

        summary.add(String.format("OK   %-20s rows=%-10d parquet=%4ds lance=%4ds",
            tag, rows, parquetSec, lanceSec));
        successes++;
      } catch (Exception ex) {
        System.err.println("=== [" + tag + "] FAILED: " + ex.getClass().getSimpleName()
            + ": " + ex.getMessage() + " ===");
        ex.printStackTrace(System.err);
        summary.add(String.format("ERR  %-20s %s: %s",
            tag, ex.getClass().getSimpleName(), ex.getMessage()));
        failures++;
      }
    }

    System.err.println("\n=== Summary ===");
    for (String line : summary) {
      System.err.println("  " + line);
    }
    System.err.println(String.format("Total: %d ok, %d failed", successes, failures));

    spark.stop();
    System.exit(failures == 0 ? 0 : 1);
  }
}
