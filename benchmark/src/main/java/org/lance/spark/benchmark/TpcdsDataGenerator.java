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
package org.lance.spark.benchmark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Spark job that generates TPC-DS data using the Kyuubi TPC-DS connector
 * and writes each table directly into the target format(s) (Lance, Parquet, etc.).
 *
 * <p>The Kyuubi connector generates data in parallel across Spark executors —
 * no external {@code dsdgen} binary or intermediate CSV/dat files are needed.
 *
 * <p>Usage:
 * <pre>
 *   spark-submit --class org.lance.spark.benchmark.TpcdsDataGenerator \
 *     benchmark.jar \
 *     --data-dir s3a://bucket/tpcds/sf10 \
 *     --scale-factor 10 \
 *     --formats parquet,partitioned-parquet,lance
 * </pre>
 */
public class TpcdsDataGenerator {

  private static final String FORMAT_LANCE = "lance";
  private static final String FORMAT_PARTITIONED_PARQUET = "partitioned-parquet";
  private static final String FORMAT_PARQUET = "parquet";
  private static final String LANCE_EXTENSION = ".lance";

  /** The 24 TPC-DS tables as named in the Kyuubi catalog. */
  static final List<String> TPCDS_TABLES =
      Arrays.asList(
          "call_center",
          "catalog_page",
          "catalog_returns",
          "catalog_sales",
          "customer",
          "customer_address",
          "customer_demographics",
          "date_dim",
          "household_demographics",
          "income_band",
          "inventory",
          "item",
          "promotion",
          "reason",
          "ship_mode",
          "store",
          "store_returns",
          "store_sales",
          "time_dim",
          "warehouse",
          "web_page",
          "web_returns",
          "web_sales",
          "web_site");

  public static void main(String[] args) throws Exception {
    String dataDir = null;
    int scaleFactor = 1;
    String formatsStr = FORMAT_PARQUET + "," + FORMAT_LANCE;
    boolean useDoubleForDecimal = false;
    String fileFormatVersion = null;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--data-dir":
          dataDir = requireValue(args, ++i, "--data-dir");
          break;
        case "--scale-factor":
          scaleFactor = Integer.parseInt(requireValue(args, ++i, "--scale-factor"));
          break;
        case "--formats":
          formatsStr = requireValue(args, ++i, "--formats");
          break;
        case "--use-double-for-decimal":
          useDoubleForDecimal = true;
          break;
        case "--file-format-version":
          fileFormatVersion = requireValue(args, ++i, "--file-format-version");
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (dataDir == null || dataDir.isBlank()) {
      System.err.println("Missing or empty required argument: --data-dir");
      printUsage();
      System.exit(1);
    }
    // Strip trailing slashes so downstream string concatenation doesn't produce "//"
    // segments in log messages or Hadoop Path inputs. The length > 1 guard preserves
    // a bare "/" (POSIX root); the regex collapses pathological inputs like
    // "s3a://bucket//" in one pass.
    if (dataDir.length() > 1) {
      dataDir = dataDir.replaceAll("/+$", "");
    }

    // Normalize and deduplicate the requested format list: trim whitespace, lower-case so
    // "Partitioned-Parquet" and "partitioned-parquet" resolve to the same on-disk directory,
    // and drop accidental duplicates ("parquet,parquet") so we don't pay the existence check
    // twice per table. After this point every caller can assume format is lower-cased.
    Set<String> formats =
        Arrays.stream(formatsStr.split(","))
            .map(s -> s.trim().toLowerCase(Locale.ROOT))
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (formats.isEmpty()) {
      System.err.println("--formats produced no recognized formats: '" + formatsStr + "'");
      printUsage();
      System.exit(1);
    }
    // Reject typos up front so the user sees a clear error rather than an opaque
    // "DataSource not found" failure mid-write after partial generation has run.
    Set<String> known = Set.of(FORMAT_PARQUET, FORMAT_PARTITIONED_PARQUET, FORMAT_LANCE);
    Set<String> unknown =
        formats.stream()
            .filter(f -> !known.contains(f))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (!unknown.isEmpty()) {
      System.err.println("Unrecognized format(s): " + unknown + ". Recognized: " + known);
      printUsage();
      System.exit(1);
    }

    // Configure Kyuubi TPC-DS catalog
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("TPC-DS Data Generator (SF=" + scaleFactor + ")")
            .config(
                "spark.sql.catalog.tpcds",
                "org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog");

    if (useDoubleForDecimal) {
      builder.config("spark.sql.catalog.tpcds.useDoubleForDecimal", "true");
    }

    SparkSession spark = builder.getOrCreate();

    try {
      System.out.println("=== TPC-DS Data Generation ===");
      System.out.println("Scale factor:  " + scaleFactor);
      System.out.println("Formats:       " + String.join(",", formats));
      System.out.println("Data dir:      " + dataDir);
      if (fileFormatVersion != null) {
        System.out.println("File format version: " + fileFormatVersion);
      }
      System.out.println();
      System.out.flush();

      String catalogDb = "tpcds.sf" + scaleFactor;

      for (String format : formats) {
        System.out.println("--- Generating " + format + " tables ---");
        System.out.flush();

        for (String table : TPCDS_TABLES) {
          generateTable(spark, catalogDb, table, format, dataDir, fileFormatVersion);
        }

        System.out.println();
      }

      System.out.println("=== Data generation complete ===");
      System.out.flush();

    } finally {
      spark.stop();
    }
  }

  private static void generateTable(
      SparkSession spark, String catalogDb, String table, String format, String dataDir,
      String fileFormatVersion) {

    boolean isLance = FORMAT_LANCE.equals(format);
    boolean isPartitionedParquet = FORMAT_PARTITIONED_PARQUET.equals(format);
    // The directory segment is the CLI format ("partitioned-parquet") so each
    // format gets its own output tree, while writeFormat below resolves to the
    // underlying Spark data source ("parquet" for the partitioned variant). The
    // two are intentionally distinct: the directory name records the operator's
    // intent, the Spark format is just the writer dispatch token.
    String tablePath = dataDir + "/" + format + "/" + table;
    if (isLance) {
      tablePath = toLancePath(tablePath) + LANCE_EXTENSION;
    }

    // hadoopPath uses the raw Hadoop scheme (e.g., abfss://); tablePath may use a
    // Lance-native scheme (e.g., az://). They diverge for Azure-Lance; the Hadoop FS
    // probe uses the former and Lance native I/O the latter.
    Path hadoopPath =
        new Path(dataDir + "/" + format + "/" + table + (isLance ? LANCE_EXTENSION : ""));
    if (skipIfComplete(spark, hadoopPath, table, isLance)) {
      return;
    }

    String partCol =
        isPartitionedParquet ? TpcdsPartitioning.partitionColumn(table).orElse(null) : null;
    String partColLabel =
        partCol != null
            ? " partitioned by " + partCol
            : (isPartitionedParquet ? " (unpartitioned dimension)" : "");
    System.out.print("  GENERATE " + table + partColLabel + "...");
    System.out.flush();
    long start = System.currentTimeMillis();

    // Read from Kyuubi TPC-DS catalog — data is generated in parallel
    Dataset<Row> df = spark.read().table(catalogDb + "." + table);

    String writeFormat = resolveWriteFormat(format);
    // Lance uses ErrorIfExists rather than Overwrite: silently overwriting a
    // possibly-valid Lance dataset on a REDO is more dangerous than failing
    // loud. A REDO triggered by a missing _versions/ marker forces the operator
    // to manually inspect/delete the partial directory before retrying.
    SaveMode mode = isLance ? SaveMode.ErrorIfExists : SaveMode.Overwrite;
    DataFrameWriter<Row> writer = buildWriter(df, partCol, mode, writeFormat);
    if (isLance && fileFormatVersion != null) {
      writer = writer.option("file_format_version", fileFormatVersion);
    }
    writer.save(tablePath);

    long elapsed = System.currentTimeMillis() - start;
    long count = readbackCount(spark, writeFormat, tablePath);
    System.out.println(" " + count + " rows (" + elapsed + "ms)");
    System.out.flush();
  }

  /**
   * Skip generation if a prior run committed successfully. Both formats look for
   * a per-format commit witness so a crashed prior run (directory present, no
   * commit) is REDONE rather than silently skipped:
   *
   * <ul>
   *   <li>Lance: the {@code _versions/} directory, written by Lance after the
   *       atomic manifest commit. Its absence means no version has been
   *       committed yet.
   *   <li>Parquet variants: the {@code _SUCCESS} marker written by the default
   *       Hadoop FileOutputCommitter on job success. Operators using a custom
   *       {@code spark.sql.sources.commitProtocolClass} that suppresses {@code
   *       _SUCCESS} will see every table redone on every invocation.
   * </ul>
   *
   * @return true if the existing output is complete and generation should skip
   */
  private static boolean skipIfComplete(
      SparkSession spark, Path hadoopPath, String table, boolean isLance) {
    String witnessName = isLance ? "_versions" : "_SUCCESS";
    try {
      FileSystem fs = hadoopPath.getFileSystem(spark.sparkContext().hadoopConfiguration());
      // Targeted exists on the witness — one HEAD request instead of a full listStatus.
      // Important on partitioned fact tables where listStatus would materialise hundreds
      // or thousands of partition-directory entries just to scan for one filename.
      if (fs.exists(new Path(hadoopPath, witnessName))) {
        System.out.println("  SKIP " + table + " (already exists at " + hadoopPath + ")");
        System.out.flush();
        return true;
      }
      // Witness absent. The TOCTOU window on this second probe only affects the
      // REDO/fresh log message, not the return value (both produce false).
      if (fs.exists(hadoopPath)) {
        System.out.println(
            "  REDO " + table + " (no " + witnessName + " marker at " + hadoopPath + ")");
        System.out.flush();
      }
      return false;
    } catch (IOException e) {
      // FS probe failed — proceed with generation rather than aborting, since the
      // write itself may still succeed (or fail with a clearer error). Log to stderr
      // so persistent issues (bad credentials surfacing as IOException subtypes such
      // as AccessDeniedException, or DNS failures) are visible rather than silent.
      System.err.println(
          "WARN: skip-check FS probe failed for "
              + hadoopPath
              + " ("
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage()
              + "); proceeding with generation");
      return false;
    }
  }

  /**
   * Builds the {@link DataFrameWriter} for one table. When a partition column is
   * supplied, NULL partition values must be dropped: Hive-style layouts cannot
   * represent them cleanly — they collapse to a single {@code
   * __HIVE_DEFAULT_PARTITION__} directory which corrupts downstream CBO stats
   * and partition pruning. The input is then pre-clustered (hash-repartition)
   * on the partition column so every row sharing one value lands on exactly
   * one task; consequently {@code partitionBy} emits one file per partition
   * directory (each value is written by a single task). Without the
   * pre-cluster, every task would hold rows for every value, producing
   * {@code numShufflePartitions × numDates} small files. TPC-DS facts exhibit
   * non-trivial peak-vs-trough skew on date_sk (busy holidays vs. ordinary
   * weekdays); operators on high scale factors should raise {@code
   * spark.sql.shuffle.partitions} or {@code spark.executor.memory} accordingly.
   *
   * @param partCol partition column name, or {@code null} for a flat write
   */
  private static DataFrameWriter<Row> buildWriter(
      Dataset<Row> df, String partCol, SaveMode mode, String writeFormat) {
    if (partCol == null) {
      return df.write().mode(mode).format(writeFormat);
    }
    Dataset<Row> clustered =
        df.filter(functions.col(partCol).isNotNull()).repartition(functions.col(partCol));
    // Pin partitionOverwriteMode=static so a REDO (Overwrite after a prior crash)
    // wipes the entire table directory rather than only the partitions touched by
    // the new run. Dynamic mode would leave orphaned partition directories from
    // the partial prior run, producing a mixed-version layout.
    return clustered
        .write()
        .mode(mode)
        .format(writeFormat)
        .option("partitionOverwriteMode", "static")
        .partitionBy(partCol);
  }

  /** Maps a CLI format to the underlying Spark write format. Caller has already lower-cased. */
  private static String resolveWriteFormat(String format) {
    if (FORMAT_PARTITIONED_PARQUET.equals(format)) {
      return FORMAT_PARQUET;
    }
    return format;
  }

  /**
   * Counts rows in the just-written table. For Lance, the dataset has its own
   * manifest and the Lance connector's {@code spark.read().format("lance")}
   * path handles empty datasets cleanly (the manifest exists even with zero
   * rows, so schema inference succeeds), so the read goes through directly.
   * For Parquet variants, an empty partitioned write commits only {@code
   * _SUCCESS} (no data files); calling {@code spark.read().load()} on a
   * marker-only directory then raises {@code AnalysisException} during schema
   * inference. The probe handles that empty case cleanly without swallowing
   * real read errors via a broad catch.
   */
  private static long readbackCount(SparkSession spark, String writeFormat, String tablePath) {
    if (FORMAT_LANCE.equals(writeFormat)) {
      return spark.read().format(FORMAT_LANCE).load(tablePath).count();
    }
    try {
      Path path = new Path(tablePath);
      FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
      if (!hasDataContent(fs, path)) {
        return 0L;
      }
    } catch (IOException e) {
      // Probe failed — fall through to the readback and let it surface the underlying
      // error, but log first so the operator sees the FS probe failure (e.g.,
      // credential issues that arrive as IOException subtypes).
      System.err.println(
          "WARN: readback FS probe failed for "
              + tablePath
              + " ("
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage()
              + "); attempting readback anyway");
    }
    return spark.read().format(writeFormat).load(tablePath).count();
  }

  /**
   * Returns true if the Parquet-style directory contains at least one
   * non-metadata entry (a partition subdirectory or a data file). Spark's
   * commit markers ({@code _SUCCESS}, {@code _temporary}) and FS sidecars
   * ({@code .crc}) are excluded. A missing path returns false rather than
   * propagating {@link FileNotFoundException}; this avoids a TOCTOU split
   * across {@code exists}/{@code listStatus} on eventually-consistent stores.
   */
  private static boolean hasDataContent(FileSystem fs, Path path) throws IOException {
    FileStatus[] children;
    try {
      children = fs.listStatus(path);
    } catch (FileNotFoundException e) {
      return false;
    }
    for (FileStatus child : children) {
      String name = child.getPath().getName();
      if (name.startsWith("_") || name.startsWith(".")) {
        continue;
      }
      return true;
    }
    return false;
  }

  /**
   * Converts Hadoop-style Azure paths to the scheme that Lance's native I/O understands.
   *
   * <p>Lance uses {@code az://} for Azure Blob Storage, while Hadoop/Spark uses
   * {@code abfss://}. The mapping is:
   *
   * <pre>
   *   abfss://container@account.dfs.core.windows.net/path
   *   →  az://container/path
   * </pre>
   *
   * <p>The storage account name must be provided separately via the
   * {@code AZURE_STORAGE_ACCOUNT_NAME} environment variable.
   *
   * <p>Non-Azure paths (local, s3://, gs://, etc.) are returned unchanged.
   */
  static String toLancePath(String path) {
    if (path == null) {
      return null;
    }
    // abfss://container@account.dfs.core.windows.net/path
    if (path.startsWith("abfss://") || path.startsWith("abfs://")) {
      String withoutScheme = path.substring(path.indexOf("://") + 3);
      int atIdx = withoutScheme.indexOf('@');
      if (atIdx < 0) {
        return path;
      }
      String container = withoutScheme.substring(0, atIdx);
      String rest = withoutScheme.substring(atIdx + 1);
      // rest = account.dfs.core.windows.net/path
      int slashIdx = rest.indexOf('/');
      String objectPath = slashIdx >= 0 ? rest.substring(slashIdx + 1) : "";
      // az://container/path — account resolved from AZURE_STORAGE_ACCOUNT_NAME env var
      String suffix = objectPath.isEmpty() ? "" : "/" + objectPath;
      return "az://" + container + suffix;
    }
    return path;
  }

  /** Reads the next CLI value; aborts with usage on a missing trailing arg. */
  private static String requireValue(String[] args, int idx, String flag) {
    if (idx >= args.length) {
      System.err.println("Missing value for argument: " + flag);
      printUsage();
      System.exit(1);
      // Defensive: if System.exit is intercepted (test harness, security manager)
      // throw rather than fall through to args[idx] and an opaque AIOBE.
      throw new IllegalStateException("unreachable");
    }
    return args[idx];
  }

  private static void printUsage() {
    System.err.println(
        "Usage: TpcdsDataGenerator"
            + " --data-dir <path>"
            + " [--scale-factor 1]"
            + " [--formats parquet,lance,partitioned-parquet]"
            + " [--use-double-for-decimal]"
            + " [--file-format-version <version>]"
            + "\n\n"
            + "Recognized formats:"
            + "\n  parquet              flat Parquet files"
            + "\n  partitioned-parquet  Hive-style partitioned Parquet (facts by date_sk)"
            + "\n  lance                Lance columnar format");
  }
}
