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
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.SparkSession;
import org.lance.spark.read.LanceColumnarPartitionReader;
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
 * TPC-DS SF=100 {@code store_sales} read benchmark — Parquet vs Lance v2.2 through toxiproxy
 * (100ms ± 5ms downstream latency) so results reflect high-RTT object-storage behavior.
 *
 * <p>Source data: 288M rows, 23 columns. On disk: Parquet 13 GB, Lance v2.2 34 GB. Uploaded to
 * {@code s3://benchmark/tpcds-sf-100/} with flat layout (no intermediate format-subdir).
 *
 * <p>Three queries, each exercising a different I/O pattern:
 * <ul>
 *   <li>{@link #q1FullscanSumQuantity}: single-column sequential sum — baseline throughput.</li>
 *   <li>{@link #q2MultiColSum}: three non-adjacent columns summed — column pruning + inter-column
 *       gap coalescing.</li>
 *   <li>{@link #q3DateRangeFilter}: range predicate on {@code ss_sold_date_sk}, ~0.5% selectivity
 *       — tests predicate pushdown and fragment / row-group pruning.</li>
 * </ul>
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>MinIO running on {@code localhost:9000}, bucket {@code benchmark} with {@code
 *       tpcds-sf-100/store_sales/} (Parquet) and {@code tpcds-sf-100/store_sales.lance/} (Lance
 *       v2.2).</li>
 *   <li>Toxiproxy on {@code localhost:19000} with the 100ms latency toxic enabled.</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   mvn exec:exec -Djmh.benchmarks=".*MinioTpcdsBenchmark.*"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
@Fork(value = 1, jvmArgs = {
    "-Xms16g",
    "-Xmx24g",
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
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Darrow.enable_unsafe_memory_access=true"
})
public class MinioTpcdsBenchmark {

  static final String BUCKET = "benchmark";
  static final String PREFIX = "tpcds-sf-100";
  static final String PROXY_ENDPOINT = "http://localhost:9000";
  static final String ACCESS_KEY = "minioadmin";
  static final String SECRET_KEY = "minioadmin";

  // ========================================================================
  // Shared Spark session — one across all scenario states so the driver JVM's
  // LanceDatasetCache accumulates warm entries across benchmarks.
  // ========================================================================

  @State(Scope.Benchmark)
  public static class TpcdsSession {
    private static volatile SparkSession SHARED;

    @Setup(Level.Trial)
    public void init() throws IOException {
      synchronized (TpcdsSession.class) {
        if (SHARED != null && !SHARED.sparkContext().isStopped()) {
          return;
        }
        printJarFingerprint();
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        SHARED = SparkSession.builder()
            .appName("lance-minio-tpcds-benchmark")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "12")
            .config("spark.sql.parquet.enableVectorizedReader", "true")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            // S3A for Parquet
            .config("spark.hadoop.fs.s3a.endpoint", PROXY_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
            .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
            // Fair-comparison tuning for Parquet over high-RTT s3a — mirrors DSRB benchmark
            .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
            .config("spark.hadoop.fs.s3a.readahead.range", "65536")
            // Lance catalog-level storage options
            .config("spark.sql.catalog.lance_default",
                "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance_default.aws_access_key_id", ACCESS_KEY)
            .config("spark.sql.catalog.lance_default.aws_secret_access_key", SECRET_KEY)
            .config("spark.sql.catalog.lance_default.aws_endpoint", PROXY_ENDPOINT)
            .config("spark.sql.catalog.lance_default.aws_region", "us-east-1")
            .config("spark.sql.catalog.lance_default.aws_virtual_hosted_style_request", "false")
            .config("spark.sql.catalog.lance_default.allow_http", "true")
            .getOrCreate();

        // Warmup Spark codegen + JIT
        SHARED.sql("SELECT sum(id) FROM range(0, 1000)").write()
            .format("noop").mode("overwrite").save();
      }
    }

    @TearDown(Level.Trial)
    public void teardown() {
      // Keep alive across per-scenario teardowns; stopped by JVM shutdown hook below.
    }

    static SparkSession get() {
      if (SHARED == null) {
        throw new IllegalStateException("SHARED SparkSession not initialized");
      }
      return SHARED;
    }

    static {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        SparkSession s = SHARED;
        if (s != null) {
          try {
            s.stop();
          } catch (Exception ignore) {
            // best effort at JVM exit
          }
          SHARED = null;
        }
      }, "minio-tpcds-spark-shutdown"));
    }
  }

  // ========================================================================
  // Path + view helpers
  // ========================================================================

  private static final String FINGERPRINT_LOG = "/tmp/jmh-fingerprint.log";

  /**
   * Prints jar/dylib/version fingerprints to stderr and appends to {@link #FINGERPRINT_LOG}.
   *
   * <p>Past two days of perf experiments were invalidated because {@code micro-benchmark/pom.xml}
   * was pinned to released {@code 0.4.0-beta.4}, so local Java edits in {@code lance-spark-base_2.12}
   * never reached the JMH classpath and Rust dylib changes were also masked by the bundled JNI lib.
   * This fingerprint surfaces the actual jar paths, file mtimes, dylib SHA-256, and Maven versions
   * so each run is verifiable BEFORE trusting any timing numbers.
   */
  static void printJarFingerprint() {
    List<String> lines = new ArrayList<>();
    lines.add("=== JMH FINGERPRINT @ " + DateTimeFormatter.ISO_INSTANT.format(Instant.now()) + " ===");

    try {
      URL classJar = LanceColumnarPartitionReader.class
          .getProtectionDomain().getCodeSource().getLocation();
      lines.add("lance-spark base jar URL : " + classJar);
      Path jarPath = toLocalPath(classJar);
      if (jarPath != null && Files.exists(jarPath)) {
        lines.add("  size  = " + Files.size(jarPath) + " bytes");
        lines.add("  mtime = " + Files.getLastModifiedTime(jarPath).toInstant()
            .atZone(ZoneId.systemDefault())
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
      } else {
        lines.add("  WARN: could not stat jar path");
      }
    } catch (Exception ex) {
      lines.add("lance-spark base jar URL : <error: " + ex + ">");
    }

    String[] dylibCandidates = {
        "nativelib/darwin-aarch64/liblance_jni.dylib",
        "nativelib/darwin-x86-64/liblance_jni.dylib",
        "nativelib/linux-aarch64/liblance_jni.so",
        "nativelib/linux-x86-64/liblance_jni.so",
    };
    String osName = System.getProperty("os.name", "").toLowerCase();
    String osArch = System.getProperty("os.arch", "").toLowerCase();
    String preferred = pickDylibResource(osName, osArch);
    lines.add("os = " + osName + " / arch = " + osArch + "  -> preferred resource: " + preferred);
    for (String resource : dylibCandidates) {
      try {
        URL r = Thread.currentThread().getContextClassLoader().getResource(resource);
        if (r == null) {
          lines.add("  resource MISSING : " + resource);
          continue;
        }
        try (InputStream in = r.openStream()) {
          byte[] bytes = in.readAllBytes();
          String sha = sha256Hex(bytes);
          String marker = resource.equals(preferred) ? " [active]" : "";
          lines.add(String.format("  resource %s : size=%d bytes  sha256=%s%s",
              resource, bytes.length, sha, marker));
        }
      } catch (Exception ex) {
        lines.add("  resource ERROR " + resource + " : " + ex);
      }
    }

    String[] pomKeys = {
        "META-INF/maven/org.lance/lance-spark-bundle-4.0_2.13/pom.properties",
        "META-INF/maven/org.lance/lance-spark-4.0_2.13/pom.properties",
        "META-INF/maven/org.lance/lance-spark-base_2.13/pom.properties",
        "META-INF/maven/org.lance/lance-spark-base_2.12/pom.properties",
        "META-INF/maven/org.lance/lance-core/pom.properties",
    };
    for (String key : pomKeys) {
      try {
        URL r = Thread.currentThread().getContextClassLoader().getResource(key);
        if (r == null) {
          lines.add("  pom MISSING : " + key);
          continue;
        }
        try (InputStream in = r.openStream()) {
          Properties p = new Properties();
          p.load(in);
          lines.add(String.format("  pom %s : groupId=%s artifactId=%s version=%s",
              key, p.getProperty("groupId"), p.getProperty("artifactId"), p.getProperty("version")));
        }
      } catch (Exception ex) {
        lines.add("  pom ERROR " + key + " : " + ex);
      }
    }
    lines.add("=== END FINGERPRINT ===");

    for (String line : lines) {
      System.err.println(line);
    }
    try {
      Files.write(Paths.get(FINGERPRINT_LOG),
          (String.join("\n", lines) + "\n").getBytes(),
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException ex) {
      System.err.println("WARN: could not append fingerprint to " + FINGERPRINT_LOG + ": " + ex);
    }
  }

  private static Path toLocalPath(URL url) {
    if (url == null) return null;
    try {
      String s = url.toString();
      if (s.startsWith("jar:")) {
        s = s.substring(4);
        int bang = s.indexOf("!");
        if (bang > 0) s = s.substring(0, bang);
      }
      if (s.startsWith("file:")) {
        return Paths.get(java.net.URI.create(s));
      }
      return Paths.get(s);
    } catch (Exception ex) {
      return null;
    }
  }

  private static String pickDylibResource(String osName, String osArch) {
    boolean isMac = osName.contains("mac") || osName.contains("darwin");
    boolean isLinux = osName.contains("linux");
    boolean isArm = osArch.contains("aarch64") || osArch.contains("arm64");
    if (isMac && isArm)   return "nativelib/darwin-aarch64/liblance_jni.dylib";
    if (isMac)            return "nativelib/darwin-x86-64/liblance_jni.dylib";
    if (isLinux && isArm) return "nativelib/linux-aarch64/liblance_jni.so";
    if (isLinux)          return "nativelib/linux-x86-64/liblance_jni.so";
    return "<unknown-platform>";
  }

  private static String sha256Hex(byte[] bytes) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
    StringBuilder sb = new StringBuilder(digest.length * 2);
    for (byte b : digest) sb.append(String.format("%02x", b));
    return sb.toString();
  }

  private static String lanceUri() {
    return "s3://" + BUCKET + "/" + PREFIX + "/store_sales.lance";
  }

  private static String lanceBp128Uri() {
    return "s3://" + BUCKET + "/" + PREFIX + "/store_sales_bp128_dispatch.lance";
  }

  private static String parquetUri() {
    return "s3a://" + BUCKET + "/" + PREFIX + "/store_sales";
  }

  /** Generic helpers for the type-coverage tables (q5+). store_sales keeps its bespoke
   *  {@code _bp128_dispatch.lance} basename for back-compat with prior runs; everything
   *  else uses the {@code _bp128} suffix produced by {@link MultiTableSetup}. */
  private static String lanceBp128Uri(String table) {
    return "s3://" + BUCKET + "/" + PREFIX + "/" + table + "_bp128.lance";
  }

  private static String parquetUri(String table) {
    return "s3a://" + BUCKET + "/" + PREFIX + "/" + table;
  }

  /** Synthetic table URIs (batch 2: types not present in TPC-DS). Stored under
   *  the {@code synth/} prefix to keep them separate from TPC-DS data. */
  private static String synthLanceUri(String table) {
    return "s3://" + BUCKET + "/synth/" + table + "_bp128.lance";
  }

  private static String synthParquetUri(String table) {
    return "s3a://" + BUCKET + "/synth/" + table;
  }

  private static void registerView(SparkSession spark, String format) {
    if ("lance".equals(format) || "lance_bp128".equals(format)) {
      String uri = "lance_bp128".equals(format) ? lanceBp128Uri() : lanceUri();
      spark.read()
          .format("lance")
          .option("block_size", "1048576")
          .option("batch_readahead", "16").option("batch_size", "65536")
          .option("aws_access_key_id", ACCESS_KEY)
          .option("aws_secret_access_key", SECRET_KEY)
          .option("aws_endpoint", PROXY_ENDPOINT)
          .option("aws_region", "us-east-1")
          .option("aws_virtual_hosted_style_request", "false")
          .option("allow_http", "true")
          .load(uri)
          .createOrReplaceTempView("store_sales");
    } else {
      spark.read()
          .format("parquet")
          .load(parquetUri())
          .createOrReplaceTempView("store_sales");
    }
  }

  /** Register a non-store_sales table (q5+) as a temp view under its plain name. */
  private static void registerSecondaryView(SparkSession spark, String format, String table) {
    if ("lance_bp128".equals(format)) {
      spark.read()
          .format("lance")
          .option("block_size", "1048576")
          .option("batch_readahead", "16").option("batch_size", "65536")
          .option("aws_access_key_id", ACCESS_KEY)
          .option("aws_secret_access_key", SECRET_KEY)
          .option("aws_endpoint", PROXY_ENDPOINT)
          .option("aws_region", "us-east-1")
          .option("aws_virtual_hosted_style_request", "false")
          .option("allow_http", "true")
          .load(lanceBp128Uri(table))
          .createOrReplaceTempView(table);
    } else {
      spark.read()
          .format("parquet")
          .load(parquetUri(table))
          .createOrReplaceTempView(table);
    }
  }

  /** Register a synthetic table (batch 2) as a temp view named {@code synth_<table>}. */
  private static void registerSynthView(SparkSession spark, String format, String table) {
    String view = "synth_" + table;
    if ("lance_bp128".equals(format)) {
      spark.read()
          .format("lance")
          .option("block_size", "1048576")
          .option("batch_readahead", "16").option("batch_size", "65536")
          .option("aws_access_key_id", ACCESS_KEY)
          .option("aws_secret_access_key", SECRET_KEY)
          .option("aws_endpoint", PROXY_ENDPOINT)
          .option("aws_region", "us-east-1")
          .option("aws_virtual_hosted_style_request", "false")
          .option("allow_http", "true")
          .load(synthLanceUri(table))
          .createOrReplaceTempView(view);
    } else {
      spark.read()
          .format("parquet")
          .load(synthParquetUri(table))
          .createOrReplaceTempView(view);
    }
  }

  private static void execute(SparkSession spark, String sql) {
    spark.sql(sql).write().format("noop").mode("overwrite").save();
  }

  // ========================================================================
  // State: format × one view per JMH trial
  // ========================================================================

  @State(Scope.Benchmark)
  public static class StoreSalesState {
    @Param({"lance_bp128", "parquet"})
    public String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new TpcdsSession().init();
      registerView(TpcdsSession.get(), format);
    }
  }

  /** State for q5+ — registers all six tables (store_sales + 5 dim/return tables) under
   *  their plain names so JOIN queries (q12) can reference both sides. */
  @State(Scope.Benchmark)
  public static class TypeCovState {
    @Param({"lance_bp128", "parquet"})
    public String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new TpcdsSession().init();
      SparkSession spark = TpcdsSession.get();
      registerView(spark, format);
      for (String table : new String[] {
          "customer", "customer_address", "item", "date_dim", "store_returns"}) {
        registerSecondaryView(spark, format, table);
      }
    }
  }

  /** State for q13+ — registers the two synthetic tables (numeric, temporal). */
  @State(Scope.Benchmark)
  public static class SyntheticState {
    @Param({"lance_bp128", "parquet"})
    public String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new TpcdsSession().init();
      SparkSession spark = TpcdsSession.get();
      registerSynthView(spark, format, "numeric");
      registerSynthView(spark, format, "temporal");
    }
  }

  // ========================================================================
  // Q1 — fullscan single column. Baseline sequential throughput.
  // ========================================================================

  @Benchmark
  public void q1FullscanSumQuantity(StoreSalesState state) {
    execute(TpcdsSession.get(), "SELECT sum(ss_quantity) FROM store_sales");
  }

  // ========================================================================
  // Q2 — three non-adjacent columns summed together. Exercises column pruning
  // and inter-column-gap coalescing. ss_item_sk is col 3, ss_quantity col 11,
  // ss_sold_date_sk col 1 — chosen as int columns to avoid the decimal128
  // fast-path NPE in this branch (Decimal.toUnscaledLong null in codegen sum).
  // The original q2 used ss_ext_sales_price (decimal); swapped to ss_sold_date_sk
  // to keep the multi-col I/O pattern while isolating PR#1 (lance-core Session
  // reuse) from unrelated decimal correctness regressions.
  // ========================================================================

  @Benchmark
  public void q2MultiColSum(StoreSalesState state) {
    execute(TpcdsSession.get(),
        "SELECT sum(ss_item_sk), sum(ss_sold_date_sk), sum(ss_quantity) FROM store_sales");
  }

  // ========================================================================
  // Q3 — date-range predicate on ss_sold_date_sk. Kyuubi-generated store_sales
  // covers ~1800 distinct date_sk values; 10 consecutive days ≈ 0.5% selectivity.
  // Tests predicate pushdown + fragment / row-group pruning via min/max stats.
  // Aggregates ss_quantity (int32) instead of ss_net_profit (decimal128) to
  // avoid the same decimal128 fast-path NPE — the predicate-pushdown signal is
  // independent of which projected column is summed.
  // ========================================================================

  @Benchmark
  public void q3DateRangeFilter(StoreSalesState state) {
    execute(TpcdsSession.get(),
        "SELECT sum(ss_quantity) FROM store_sales "
            + "WHERE ss_sold_date_sk BETWEEN 2451180 AND 2451189");
  }

  // ========================================================================
  // Q4 — six decimal128 columns summed together. Designed to isolate the
  // benefit of LanceDecimalAccessor's u128 fast path (precision<=18 → read
  // unscaled long directly from Arrow buffer, skip BigInteger allocation).
  // ========================================================================

  @Benchmark
  public void q4DecimalSum(StoreSalesState state) {
    execute(TpcdsSession.get(),
        "SELECT sum(ss_wholesale_cost), sum(ss_list_price), sum(ss_sales_price), "
            + "sum(ss_ext_sales_price), sum(ss_net_paid), sum(ss_net_profit) "
            + "FROM store_sales");
  }

  // ========================================================================
  // Q5 — int32 high-cardinality count distinct on PK column. Exercises the
  // dictionary-build cost during fullscan + hashAgg of int values (no
  // decimal/varchar overhead). customer SF=100 is ~2 M rows.
  // ========================================================================

  @Benchmark
  public void q5IntCountDistinct(TypeCovState state) {
    execute(TpcdsSession.get(),
        "SELECT count(distinct c_customer_sk) FROM customer");
  }

  // ========================================================================
  // Q6 — two decimal(7,2) columns summed on store_returns (~28 M rows, 10x
  // smaller than store_sales). Mirrors q4's decimal hot path on a different
  // table; sr_fee + sr_return_tax both go through the same low-precision
  // u32-narrow bp128 kernel.
  // ========================================================================

  @Benchmark
  public void q6DecimalReturnsSum(TypeCovState state) {
    execute(TpcdsSession.get(),
        "SELECT sum(sr_fee), sum(sr_return_tax) FROM store_returns");
  }

  // ========================================================================
  // Q7 — varchar(50) high-cardinality stress. count(distinct i_product_name)
  // forces a fullscan + hash-agg over BinaryMiniBlock-encoded data. item ~200K
  // rows, but every name is distinct, so this measures string materialization
  // throughput more than aggregation cost.
  // ========================================================================

  @Benchmark
  public void q7VarcharDistinct(TypeCovState state) {
    execute(TpcdsSession.get(),
        "SELECT count(distinct i_product_name) FROM item");
  }

  // ========================================================================
  // Q8 — varchar equality filter. count(*) WHERE c_first_name='James' on
  // customer (~2 M rows). Evaluates predicate pushdown + string compare.
  // 'James' is one of the more common first names in dsdgen output.
  // ========================================================================

  @Benchmark
  public void q8VarcharFilter(TypeCovState state) {
    execute(TpcdsSession.get(),
        "SELECT count(*) FROM customer WHERE c_first_name = 'James'");
  }

  // ========================================================================
  // Q9 — char(2) low-cardinality GROUP BY. customer_address.ca_state has
  // ~50 distinct values across ~1 M rows. Tests fixed-width string materialization
  // + hash-agg with small group set (no spill).
  // ========================================================================

  @Benchmark
  public void q9CharGroupby(TypeCovState state) {
    execute(TpcdsSession.get(),
        "SELECT ca_state, count(*) FROM customer_address GROUP BY ca_state");
  }

  // ========================================================================
  // Q10 — date32 range filter. count(*) WHERE d_date BETWEEN ... over
  // date_dim (~73 K rows). Lance stores date as i32 days-since-epoch and
  // routes through bp128 u32-narrow; Parquet stores as INT32-DATE.
  // ========================================================================

  @Benchmark
  public void q10DateRange(TypeCovState state) {
    execute(TpcdsSession.get(),
        "SELECT count(*) FROM date_dim "
            + "WHERE d_date BETWEEN DATE'1999-01-01' AND DATE'2002-12-31'");
  }

  // ========================================================================
  // Q11 — int32 low-cardinality GROUP BY on date_dim.d_year. ~5 distinct
  // years, ~73 K rows. Tests the agg path on tiny dimension table — exposes
  // setup/teardown overhead per query.
  // ========================================================================

  @Benchmark
  public void q11DateGroupby(TypeCovState state) {
    execute(TpcdsSession.get(),
        "SELECT d_year, count(*) FROM date_dim GROUP BY d_year");
  }

  // ========================================================================
  // Q12 — typical OLAP star-join: store_sales × date_dim on int32 surrogate
  // key. SUM(ss_quantity) GROUP BY d_year. Exercises both fact and dim tables
  // simultaneously; broadcast join on date_dim (small) is the expected plan.
  // ========================================================================

  @Benchmark
  public void q12JoinIntPk(TypeCovState state) {
    execute(TpcdsSession.get(),
        "SELECT d_year, sum(ss_quantity) "
            + "FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk "
            + "GROUP BY d_year");
  }

  // ========================================================================
  // Q13-Q19 — synthetic types not present in TPC-DS. 30 M-row tables.
  // Each query is a simple aggregation isolating one logical type.
  // ========================================================================

  @Benchmark
  public void q13Int64Sum(SyntheticState state) {
    // max() instead of sum() to avoid Spark ANSI overflow on 30M × ~5e14 → > long range.
    // Read path is identical: i64 column fullscan + scalar reduce.
    execute(TpcdsSession.get(), "SELECT max(v_int64) FROM synth_numeric");
  }

  @Benchmark
  public void q14DoubleSum(SyntheticState state) {
    execute(TpcdsSession.get(), "SELECT sum(v_double) FROM synth_numeric");
  }

  @Benchmark
  public void q15FloatSum(SyntheticState state) {
    execute(TpcdsSession.get(), "SELECT sum(v_float) FROM synth_numeric");
  }

  @Benchmark
  public void q16Decimal18Sum(SyntheticState state) {
    execute(TpcdsSession.get(), "SELECT sum(v_decimal_18_2) FROM synth_numeric");
  }

  // q17 forces wide-bit-width path: data has unscaled values up to ~10^36 ≈ 120 bits,
  // which routes through bp128 SequentialU128 / Memcpy kernel.
  // Uses max() instead of sum() because 30M × 10^36 overflows decimal(38,18) under ANSI mode.
  @Benchmark
  public void q17Decimal38Sum(SyntheticState state) {
    execute(TpcdsSession.get(), "SELECT max(v_decimal_38_18) FROM synth_numeric");
  }

  @Benchmark
  public void q18TimestampRange(SyntheticState state) {
    execute(TpcdsSession.get(),
        "SELECT count(*) FROM synth_temporal "
            + "WHERE v_timestamp BETWEEN TIMESTAMP'2018-01-01 00:00:00' "
            + "AND TIMESTAMP'2020-01-01 00:00:00'");
  }

  @Benchmark
  public void q19BooleanCount(SyntheticState state) {
    execute(TpcdsSession.get(),
        "SELECT sum(CASE WHEN v_boolean THEN 1 ELSE 0 END) FROM synth_numeric");
  }
}