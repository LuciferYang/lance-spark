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
 * Local vs local-cluster A/B for the C1 dataset cache. Runs a curated subset of queries 30
 * iterations each in one SparkSession, alternating noop+count+collect to avoid the
 * noop-loop accumulation artifact that JMH's repeated-noop measurement triggered.
 *
 * <p>{@code -Dspark.master.override=local-cluster[N,C,M]} switches Spark from {@code local[*]}
 * to local-cluster, forking N executor JVMs. Executor JVMs receive the same {@code --add-opens}
 * set as the driver via {@code spark.executor.extraJavaOptions} so Arrow off-heap memory access
 * works on each executor.
 *
 * <p>Reports per-iter {@code noop_ms count_ms collect_ms} per query; iter 10-30 mean is the
 * steady-state to compare across master modes.
 */
public class LocalClusterAB {

  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";

  /** {tag, viewName, sql} */
  private static final Map<String, String[]> QUERIES = new LinkedHashMap<>();

  static {
    QUERIES.put("q1",
        new String[] {"store_sales", "SELECT sum(ss_quantity) FROM store_sales"});
    QUERIES.put("q2",
        new String[] {"store_sales",
            "SELECT sum(ss_item_sk), sum(ss_sold_date_sk), sum(ss_quantity) FROM store_sales"});
    QUERIES.put("q3",
        new String[] {"store_sales",
            "SELECT sum(ss_quantity) FROM store_sales "
                + "WHERE ss_sold_date_sk BETWEEN 2451180 AND 2451189"});
    QUERIES.put("q4",
        new String[] {"store_sales",
            "SELECT sum(ss_wholesale_cost), sum(ss_list_price), sum(ss_sales_price), "
                + "sum(ss_ext_sales_price), sum(ss_net_paid), sum(ss_net_profit) "
                + "FROM store_sales"});
    QUERIES.put("q11",
        new String[] {"date_dim", "SELECT d_year, count(*) FROM date_dim GROUP BY d_year"});
    QUERIES.put("q14",
        new String[] {"synth_numeric", "SELECT sum(v_double) FROM synth_numeric"});
  }

  /** {viewName -> uri-suffix} so a view can map to either a Lance or Parquet URI by format. */
  private static final Map<String, String> VIEWS = new LinkedHashMap<>();

  static {
    VIEWS.put("store_sales", "tpcds-sf-100/store_sales");
    VIEWS.put("date_dim", "tpcds-sf-100/date_dim");
    VIEWS.put("synth_numeric", "synth/numeric");
  }

  private static String executorJavaOptions() {
    return String.join(" ",
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
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Darrow.enable_unsafe_memory_access=true",
        "-Dlance.spark.dataset_cache_enabled="
            + System.getProperty("lance.spark.dataset_cache_enabled", "true"),
        "-Dlance.spark.probe_fixed_cost="
            + System.getProperty("lance.spark.probe_fixed_cost", "false"));
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

    String master = System.getProperty("spark.master.override", "local[*]");
    System.err.println("=== LocalClusterAB master=" + master + " format=" + format
        + " iterations=" + iterations + " ===");

    SparkSession.Builder builder = SparkSession.builder()
        .appName("local-cluster-ab")
        .master(master)
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
        .config("spark.sql.catalog.lance_default.allow_http", "true");

    if (master.startsWith("local-cluster")) {
      builder.config("spark.executor.extraJavaOptions", executorJavaOptions());
      // Distribute the running jar to executors so they have the lance-spark-bundle
      // and benchmark classes on classpath.
      String jvmCp = System.getProperty("java.class.path", "");
      builder.config("spark.executor.extraClassPath", jvmCp);
      builder.config("spark.driver.extraClassPath", jvmCp);
      // Worker / Executor processes need SPARK_HOME to compute the Spark classpath
      // for their forked JVM (AbstractCommandBuilder.getScalaVersion looks under
      // $SPARK_HOME/jars). Propagate via spark.{worker,executor}Env so it is set
      // in the spawned process environment, and via spark.home so SparkLauncher
      // can also pick it up directly.
      String sparkHome = System.getenv("SPARK_HOME");
      if (sparkHome == null || sparkHome.isEmpty()) {
        sparkHome = System.getProperty("spark.home");
      }
      if (sparkHome != null && !sparkHome.isEmpty()) {
        builder.config("spark.home", sparkHome);
        builder.config("spark.executorEnv.SPARK_HOME", sparkHome);
        builder.config("spark.workerEnv.SPARK_HOME", sparkHome);
        // AbstractCommandBuilder.getScalaVersion() looks for SPARK_HOME/launcher/target/scala-2.13
        // which only exists in a dev tree, not a binary distribution. Setting
        // SPARK_SCALA_VERSION skips the directory probe entirely.
        builder.config("spark.executorEnv.SPARK_SCALA_VERSION", "2.13");
        builder.config("spark.workerEnv.SPARK_SCALA_VERSION", "2.13");
        System.err.println("=== propagating SPARK_HOME=" + sparkHome
            + " + SPARK_SCALA_VERSION=2.13 to workers/executors ===");
      } else {
        System.err.println("WARN: SPARK_HOME not set; local-cluster will likely fail with "
            + "'Cannot find any build directories'");
      }
    }

    SparkSession spark = builder.getOrCreate();

    // Register all needed views once.
    boolean isLance = "lance_bp128".equals(format);
    for (Map.Entry<String, String> v : VIEWS.entrySet()) {
      String view = v.getKey();
      String slug = v.getValue();
      String uri;
      if (isLance) {
        // store_sales uses the bespoke _bp128_dispatch.lance basename.
        if ("store_sales".equals(view)) {
          uri = "s3://benchmark/tpcds-sf-100/store_sales_bp128_dispatch.lance";
        } else if (slug.startsWith("synth/")) {
          uri = "s3://benchmark/" + slug.substring(0, slug.lastIndexOf('/') + 1)
              + slug.substring(slug.lastIndexOf('/') + 1) + "_bp128.lance";
        } else {
          // tpcds-sf-100/<table> -> s3://benchmark/tpcds-sf-100/<table>_bp128.lance
          uri = "s3://benchmark/" + slug + "_bp128.lance";
        }
      } else {
        uri = "s3a://benchmark/" + slug;
      }
      try {
        if (isLance) {
          spark.read().format("lance")
              .option("aws_access_key_id", ACCESS_KEY)
              .option("aws_secret_access_key", SECRET_KEY)
              .option("aws_endpoint", MINIO_ENDPOINT)
              .option("aws_region", "us-east-1")
              .option("aws_virtual_hosted_style_request", "false")
              .option("allow_http", "true")
              .load(uri)
              .createOrReplaceTempView(view);
        } else {
          spark.read().format("parquet").load(uri).createOrReplaceTempView(view);
        }
      } catch (Exception ex) {
        System.err.println("WARN: failed to register view " + view + " from " + uri
            + ": " + ex.getMessage());
      }
    }

    System.out.println(">>> q\tformat\tmaster\titer\tnoop_ms\tcount_ms\tcollect_ms");
    for (Map.Entry<String, String[]> e : QUERIES.entrySet()) {
      String tag = e.getKey();
      String sql = e.getValue()[1];
      if (only != null && !only.contains(tag)) continue;
      String masterTag = master.startsWith("local-cluster") ? "local-cluster" : "local";
      for (int i = 1; i <= iterations; i++) {
        long t0 = System.nanoTime();
        spark.sql(sql).write().format("noop").mode("overwrite").save();
        long noopMs = (System.nanoTime() - t0) / 1_000_000L;

        long t1 = System.nanoTime();
        spark.sql(sql).count();
        long countMs = (System.nanoTime() - t1) / 1_000_000L;

        long t2 = System.nanoTime();
        spark.sql(sql).collectAsList().size();
        long collectMs = (System.nanoTime() - t2) / 1_000_000L;

        System.out.printf(">>> %s\t%s\t%s\t%d\t%d\t%d\t%d%n",
            tag, format, masterTag, i, noopMs, countMs, collectMs);
      }
    }

    spark.stop();
  }
}
