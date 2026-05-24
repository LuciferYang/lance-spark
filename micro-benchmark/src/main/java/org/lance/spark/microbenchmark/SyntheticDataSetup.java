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
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

/**
 * Generates two synthetic Lance + Parquet datasets for the type-coverage matrix that TPC-DS
 * doesn't have built-in: int64, float, double, wide decimal128, timestamp(micros), boolean.
 *
 * <h3>Tables</h3>
 * <ul>
 *   <li>{@code synth/numeric_bp128.lance} (and Parquet equivalent) — 30 M rows, 7 columns:
 *     <ul>
 *       <li>{@code id}            bigint     0..N-1</li>
 *       <li>{@code v_int64}       bigint     uniform [0, 10^15)</li>
 *       <li>{@code v_double}      double     uniform [-1e6, 1e6)</li>
 *       <li>{@code v_float}       float      uniform [-1e6, 1e6)</li>
 *       <li>{@code v_decimal_18_2} decimal(18,2)  uniform unscaled [0, 10^16) — fits NarrowU64 kernel</li>
 *       <li>{@code v_decimal_38_18} decimal(38,18) uniform unscaled [0, 10^36) — wide enough to
 *           force SequentialU128 / Memcpy kernel</li>
 *       <li>{@code v_boolean}     boolean    uniform 50/50</li>
 *     </ul></li>
 *   <li>{@code synth/temporal_bp128.lance} — 30 M rows, 2 columns:
 *     <ul>
 *       <li>{@code id}             bigint   0..N-1</li>
 *       <li>{@code v_timestamp}    timestamp(micros) uniform across 2015-01-01 .. 2025-01-01</li>
 *     </ul></li>
 * </ul>
 *
 * <p>Run via:
 * <pre>
 *   java ... org.lance.spark.microbenchmark.SyntheticDataSetup [overwrite] [only=numeric,temporal]
 * </pre>
 */
public class SyntheticDataSetup {

  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";

  private static final long ROW_COUNT = 30_000_000L;
  private static final long SEED_NUMERIC = 0xC0FFEEL; // arbitrary, derived seeds add offsets
  private static final long SEED_TEMPORAL = 0xCAFEBABEL;

  // 2015-01-01 00:00:00 UTC = 1420070400 sec; 2025-01-01 = 1735689600 sec.
  // Range = 315619200 sec ≈ 10 yr.
  private static final long TS_START_SEC = 1420070400L;
  private static final long TS_RANGE_SEC = 315619200L;

  public static void main(String[] args) {
    boolean overwrite = false;
    java.util.Set<String> only = null;
    for (String arg : args) {
      if ("overwrite".equalsIgnoreCase(arg)) {
        overwrite = true;
      } else if (arg.startsWith("only=")) {
        only = new java.util.HashSet<>();
        for (String t : arg.substring("only=".length()).split(",")) only.add(t.trim());
      }
    }
    SaveMode saveMode = overwrite ? SaveMode.Overwrite : SaveMode.ErrorIfExists;

    SparkSession spark = SparkSession.builder()
        .appName("synth-data-setup")
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
        .config("spark.sql.catalog.lance_default",
            "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance_default.aws_access_key_id", ACCESS_KEY)
        .config("spark.sql.catalog.lance_default.aws_secret_access_key", SECRET_KEY)
        .config("spark.sql.catalog.lance_default.aws_endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.lance_default.aws_region", "us-east-1")
        .config("spark.sql.catalog.lance_default.aws_virtual_hosted_style_request", "false")
        .config("spark.sql.catalog.lance_default.allow_http", "true")
        .getOrCreate();

    java.util.List<String> summary = new java.util.ArrayList<>();
    int failures = 0;

    if (only == null || only.contains("numeric")) {
      try {
        long t0 = System.nanoTime();
        Dataset<Row> df = buildNumeric(spark);
        writeBoth(df, "numeric", saveMode, summary);
        System.err.println("numeric build+write total elapsed="
            + (System.nanoTime() - t0) / 1_000_000_000L + "s");
      } catch (Exception ex) {
        System.err.println("numeric FAILED: " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
        ex.printStackTrace(System.err);
        summary.add("ERR  numeric  " + ex.getMessage());
        failures++;
      }
    }

    if (only == null || only.contains("temporal")) {
      try {
        long t0 = System.nanoTime();
        Dataset<Row> df = buildTemporal(spark);
        writeBoth(df, "temporal", saveMode, summary);
        System.err.println("temporal build+write total elapsed="
            + (System.nanoTime() - t0) / 1_000_000_000L + "s");
      } catch (Exception ex) {
        System.err.println("temporal FAILED: " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
        ex.printStackTrace(System.err);
        summary.add("ERR  temporal  " + ex.getMessage());
        failures++;
      }
    }

    System.err.println("\n=== Summary ===");
    for (String line : summary) System.err.println("  " + line);

    spark.stop();
    System.exit(failures == 0 ? 0 : 1);
  }

  private static Dataset<Row> buildNumeric(SparkSession spark) {
    // Use Spark's range + rand(seed) for deterministic generation.
    // rand returns double in [0, 1).
    return spark.range(0, ROW_COUNT, 1, 8)
        .withColumn("v_int64",
            expr("cast(rand(" + SEED_NUMERIC + ") * 1.0e15 as bigint)"))
        .withColumn("v_double",
            expr("(rand(" + (SEED_NUMERIC + 1) + ") - 0.5) * 2.0e6"))
        .withColumn("v_float",
            expr("cast((rand(" + (SEED_NUMERIC + 2) + ") - 0.5) * 2.0e6 as float)"))
        .withColumn("v_decimal_18_2",
            // Unscaled in [0, 10^16) → decimal(18,2) nominal range up to 10^14
            expr("cast(cast(rand(" + (SEED_NUMERIC + 3) + ") * 1.0e14 as decimal(18,2)) as decimal(18,2))"))
        .withColumn("v_decimal_38_18",
            // Force wide bit-width: build value as integerBigPart.fractionPart with 18 fractional digits.
            // The integer part spans [0, 10^18), so combined unscaled ranges up to ~10^36 which
            // requires ≥120 bits → falls into SequentialU128 / Memcpy kernel.
            expr("cast(cast(rand(" + (SEED_NUMERIC + 4) + ") * 1.0e18 as decimal(38,0)) "
                + "+ cast(rand(" + (SEED_NUMERIC + 5) + ") as decimal(38,18)) as decimal(38,18))"))
        .withColumn("v_boolean",
            expr("rand(" + (SEED_NUMERIC + 6) + ") < 0.5"));
  }

  private static Dataset<Row> buildTemporal(SparkSession spark) {
    return spark.range(0, ROW_COUNT, 1, 8)
        .withColumn("v_timestamp",
            expr("timestamp_seconds(cast(" + TS_START_SEC + " + rand(" + SEED_TEMPORAL + ") * "
                + TS_RANGE_SEC + " as bigint))"));
  }

  private static void writeBoth(Dataset<Row> df, String tag, SaveMode saveMode,
      java.util.List<String> summary) {
    String parquetUri = "s3a://benchmark/synth/" + tag + "/";
    String lanceUri = "s3://benchmark/synth/" + tag + "_bp128.lance";

    System.err.println("\n=== [" + tag + "] writing parquet: " + parquetUri + " ===");
    long t0 = System.nanoTime();
    df.write().format("parquet").mode(saveMode).save(parquetUri);
    long parquetSec = (System.nanoTime() - t0) / 1_000_000_000L;
    System.err.println("=== [" + tag + "] parquet done, elapsed=" + parquetSec + "s ===");

    System.err.println("=== [" + tag + "] writing lance: " + lanceUri + " ===");
    t0 = System.nanoTime();
    df.write()
        .format("lance")
        .option("aws_access_key_id", ACCESS_KEY)
        .option("aws_secret_access_key", SECRET_KEY)
        .option("aws_endpoint", MINIO_ENDPOINT)
        .option("aws_virtual_hosted_style_request", "false")
        .option("aws_region", "us-east-1")
        .option("allow_http", "true")
        .mode(saveMode)
        .save(lanceUri);
    long lanceSec = (System.nanoTime() - t0) / 1_000_000_000L;
    System.err.println("=== [" + tag + "] lance done, elapsed=" + lanceSec + "s ===");

    summary.add(String.format("OK   %-12s parquet=%4ds lance=%4ds", tag, parquetSec, lanceSec));
  }
}
