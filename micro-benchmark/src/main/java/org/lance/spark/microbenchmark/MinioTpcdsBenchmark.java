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

  private static void execute(SparkSession spark, String sql) {
    spark.sql(sql).write().format("noop").mode("overwrite").save();
  }

  // ========================================================================
  // State: format × one view per JMH trial
  // ========================================================================

  @State(Scope.Benchmark)
  public static class StoreSalesState {
    @Param({"lance", "lance_bp128", "parquet"})
    public String format;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      new TpcdsSession().init();
      registerView(TpcdsSession.get(), format);
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
}
