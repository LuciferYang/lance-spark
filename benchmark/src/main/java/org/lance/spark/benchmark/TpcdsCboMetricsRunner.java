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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * CBO metrics harness for TPC-DS on Lance.
 *
 * <p>Runs each TPC-DS query against the Lance data source and captures (per query):
 *
 * <ul>
 *   <li>{@code EXPLAIN COST} text — full Catalyst output including {@code Statistics(...)} lines.
 *   <li>Physical plan operator counts (BroadcastHashJoin, SortMergeJoin, ShuffledHashJoin, etc.).
 *   <li>Lance scan count and a coarse "reports column stats" indicator scraped from the EXPLAIN
 *       COST text.
 *   <li>Wall-clock runtime (median of N iterations) and shuffle read/write bytes.
 *   <li>Status (OK / FAIL) plus error message for failures.
 * </ul>
 *
 * <p>Output (under {@code <results-dir>/cbo-runs/<run-label>/}):
 *
 * <ul>
 *   <li>{@code runs.jsonl} — one JSON object per query (fed into {@link CboMetricsReporter}).
 *   <li>{@code plans/<query>.cost.txt} — raw EXPLAIN COST text per query.
 *   <li>{@code plans/<query>.physical.txt} — executed physical plan toString per query.
 *   <li>{@code summary.csv} — flat per-query metrics for spreadsheet eyeballing.
 * </ul>
 *
 * <p>This is the "before" / "after" data point for Phase 1 column-stats reporting. Run it once
 * before Phase 1 lands (baseline) and again after; diff the two run directories.
 */
public class TpcdsCboMetricsRunner {

  private static final DateTimeFormatter RUN_ID_FMT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

  /** Pattern matching {@code Statistics(...)} blocks in EXPLAIN COST output. */
  private static final Pattern STATISTICS_LINE =
      Pattern.compile("Statistics\\([^)]*\\)", Pattern.MULTILINE);

  /** Pattern matching {@code colStats=Map(...)} fragments — non-empty Map indicates real stats. */
  private static final Pattern COL_STATS_MAP =
      Pattern.compile("colStats=Map\\(([^)]*)\\)");

  /** Pattern recognising lines that scan Lance tables. */
  private static final Pattern LANCE_SCAN_LINE =
      Pattern.compile("(?i)BatchScan.*Lance|RelationV2.*Lance|LanceScan");

  public static void main(String[] args) throws Exception {
    Args parsed = Args.parse(args);

    String runId = parsed.runLabel != null ? parsed.runLabel
        : "run-" + LocalDateTime.now().format(RUN_ID_FMT);
    Path runDir = Paths.get(parsed.resultsDir, "cbo-runs", runId);
    Path plansDir = runDir.resolve("plans");
    Files.createDirectories(plansDir);

    System.out.println("=== TPC-DS CBO Metrics Runner ===");
    System.out.println("Data dir:      " + parsed.dataDir);
    System.out.println("Results dir:   " + parsed.resultsDir);
    System.out.println("Run id:        " + runId);
    System.out.println("Iterations:    " + parsed.iterations);
    System.out.println("Query filter:  " + (parsed.queryFilter != null ? parsed.queryFilter : "(all)"));
    System.out.println();
    System.out.flush();

    SparkSession.Builder builder = SparkSession.builder().appName("TPC-DS CBO Metrics");
    builder.config("spark.sql.cbo.enabled", "true");
    builder.config("spark.sql.cbo.joinReorder.enabled", "true");
    builder.config("spark.sql.cbo.planStats.enabled", "true");
    builder.config("spark.sql.adaptive.enabled", "true");
    SparkSession spark = builder.getOrCreate();

    QueryMetricsListener metricsListener = new QueryMetricsListener();
    spark.sparkContext().addSparkListener(metricsListener);

    Path jsonlPath = runDir.resolve("runs.jsonl");
    Path summaryCsvPath = runDir.resolve("summary.csv");

    try (BufferedWriter jsonl = Files.newBufferedWriter(jsonlPath, StandardCharsets.UTF_8);
        BufferedWriter csv = Files.newBufferedWriter(summaryCsvPath, StandardCharsets.UTF_8)) {

      csv.write(
          "query,status,iterations,median_ms,min_ms,max_ms,shuffle_read_bytes,"
              + "shuffle_write_bytes,bhj,smj,shj,lance_scans,col_stats_reported\n");

      TpcdsDataLoader loader = new TpcdsDataLoader(spark, parsed.dataDir);
      loader.registerTables("lance");

      List<String> queryNames = filterQueries(getAvailableQueries(), parsed.queryFilter);
      System.out.println("Running " + queryNames.size() + " queries x "
          + parsed.iterations + " iterations against Lance");
      System.out.flush();

      int idx = 0;
      for (String queryName : queryNames) {
        idx++;
        System.out.printf("[%d/%d] %s ", idx, queryNames.size(), queryName);
        System.out.flush();

        String sql = loadQuery(queryName);
        if (sql == null) {
          System.out.println("(no SQL)");
          continue;
        }

        QueryRecord record = runOne(spark, queryName, sql, parsed.iterations,
            metricsListener, plansDir, parsed.timeoutSeconds);
        jsonl.write(record.toJson());
        jsonl.write('\n');
        jsonl.flush();
        csv.write(record.toCsvRow());
        csv.write('\n');
        csv.flush();

        System.out.printf("status=%s median=%dms bhj=%d smj=%d shj=%d lance_scans=%d colstats=%s%n",
            record.status, record.medianMs, record.bhjCount, record.smjCount, record.shjCount,
            record.lanceScanCount, record.colStatsReported ? "Y" : "N");
        System.out.flush();
      }

      loader.unregisterTables();
    } finally {
      spark.stop();
    }

    System.out.println();
    System.out.println("Outputs written to: " + runDir);
    System.out.println("  - runs.jsonl");
    System.out.println("  - summary.csv");
    System.out.println("  - plans/<query>.cost.txt and .physical.txt");
  }

  private static QueryRecord runOne(SparkSession spark, String queryName, String sql,
      int iterations, QueryMetricsListener metricsListener, Path plansDir, int timeoutSeconds) {
    QueryRecord record = new QueryRecord();
    record.queryName = queryName;

    String[] statements = splitStatements(sql);
    String firstStatement = firstNonEmpty(statements);

    // Capture EXPLAIN COST on the first (longest) statement — for multi-statement queries this
    // yields the root WITH/SELECT plan, which is what CBO operates on. Wrapped in a future
    // because logical-plan stats estimation can hang on pathological multi-way joins.
    if (firstStatement != null) {
      ExecutorService planExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "cbo-plan-capture-" + queryName);
        t.setDaemon(true);
        return t;
      });
      final String stmt = firstStatement;
      try {
        Future<Void> planFuture = planExecutor.submit(() -> {
          try {
            Dataset<Row> costDf = spark.sql("EXPLAIN COST " + stmt);
            StringBuilder cost = new StringBuilder();
            for (Row row : (Row[]) costDf.collect()) {
              cost.append(row.mkString());
              cost.append('\n');
            }
            record.explainCostText = cost.toString();
            writeText(plansDir.resolve(queryName + ".cost.txt"), record.explainCostText);
          } catch (Exception e) {
            record.explainCostText = "EXPLAIN COST failed: " + e.getMessage();
          }
          try {
            Dataset<Row> df = spark.sql(stmt);
            // Use sparkPlan() (pre-AQE) so we see the full physical tree. executedPlan() returns
            // an AdaptiveSparkPlanExec wrapper whose children() is empty under AQE.
            SparkPlan sparkPlan = df.queryExecution().sparkPlan();
            record.physicalPlanText = sparkPlan.toString();
            writeText(plansDir.resolve(queryName + ".physical.txt"), record.physicalPlanText);
            analyzePlan(sparkPlan, record);

            // Probe Catalyst-level attributeStats on the optimized logical plan — bypasses
            // EXPLAIN COST simpleString in case Spark's formatter omits colStats. We walk the
            // tree, count any plan node whose .stats.attributeStats is non-empty.
            try {
              org.apache.spark.sql.catalyst.plans.logical.LogicalPlan optimized =
                  df.queryExecution().optimizedPlan();
              int[] tally = new int[1];
              optimized.foreach(node -> {
                if (node.stats().attributeStats() != null
                    && !node.stats().attributeStats().isEmpty()) {
                  tally[0] += node.stats().attributeStats().size();
                }
                return null;
              });
              if (tally[0] > 0) {
                record.colStatsReported = true;
                record.colStatsAttributeCount = tally[0];
              }
            } catch (Exception ignored) {
              // Best-effort probe — fall back to the regex-based detection below.
            }
          } catch (Exception e) {
            record.physicalPlanText = "physical plan capture failed: " + e.getMessage();
          }
          return null;
        });
        try {
          planFuture.get(Math.min(120, timeoutSeconds), TimeUnit.SECONDS);
        } catch (TimeoutException te) {
          planFuture.cancel(true);
          record.explainCostText = "EXPLAIN COST/plan capture timed out";
          record.physicalPlanText = "(timeout)";
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          record.explainCostText = "EXPLAIN COST interrupted";
          record.physicalPlanText = "(interrupted)";
        } catch (Exception e) {
          record.explainCostText = "EXPLAIN COST failed: " + e.getMessage();
          record.physicalPlanText = "(failed)";
        }
      } finally {
        planExecutor.shutdownNow();
      }

      // Programmatic Catalyst probe is authoritative; the regex fallback is only consulted when
      // the probe didn't fire (e.g., when EXPLAIN COST timed out before we could capture the
      // optimized plan).
      if (!record.colStatsReported) {
        record.colStatsReported = detectColStats(record.explainCostText);
      }
      record.lanceScanCountFromExplain = countLanceScans(record.explainCostText);
    }

    // Run the full multi-statement query N times, capture timing distribution.
    long[] times = new long[iterations];
    boolean ok = true;
    String errorMessage = null;
    long lastShuffleRead = 0;
    long lastShuffleWrite = 0;

    ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "cbo-query-runner-" + queryName);
      t.setDaemon(true);
      return t;
    });

    try {
      for (int i = 0; i < iterations; i++) {
        String jobGroup = "cbo." + queryName + ".iter" + (i + 1);
        spark.sparkContext().setJobGroup(jobGroup, queryName, true);
        metricsListener.reset(jobGroup);

        long start = System.currentTimeMillis();
        Future<Void> future = executor.submit(() -> {
          for (String stmt : statements) {
            String trimmed = stmt.trim();
            if (trimmed.isEmpty()) {
              continue;
            }
            Dataset<Row> result = spark.sql(trimmed);
            result.write().format("noop").mode("overwrite").save();
          }
          return null;
        });

        try {
          future.get(timeoutSeconds, TimeUnit.SECONDS);
          times[i] = System.currentTimeMillis() - start;
          QueryMetrics metrics = metricsListener.getMetrics();
          if (metrics != null) {
            lastShuffleRead = metrics.getShuffleReadBytes();
            lastShuffleWrite = metrics.getShuffleWriteBytes();
          }
        } catch (TimeoutException te) {
          spark.sparkContext().cancelJobGroup(jobGroup);
          future.cancel(true);
          ok = false;
          errorMessage = "TIMEOUT after " + timeoutSeconds + "s";
          times[i] = System.currentTimeMillis() - start;
          break;
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          spark.sparkContext().cancelJobGroup(jobGroup);
          future.cancel(true);
          ok = false;
          errorMessage = "INTERRUPTED";
          times[i] = System.currentTimeMillis() - start;
          break;
        } catch (Exception e) {
          ok = false;
          Throwable cause = e.getCause() != null ? e.getCause() : e;
          String msg = cause.getMessage();
          errorMessage = msg != null && msg.length() > 500 ? msg.substring(0, 500) + "..." : msg;
          times[i] = System.currentTimeMillis() - start;
          break;
        } finally {
          spark.sparkContext().clearJobGroup();
        }
      }
    } finally {
      executor.shutdownNow();
    }

    record.status = ok ? "OK" : "FAIL";
    record.errorMessage = errorMessage;
    record.iterations = iterations;
    if (ok) {
      long[] sorted = times.clone();
      Arrays.sort(sorted);
      record.medianMs = sorted[sorted.length / 2];
      record.minMs = sorted[0];
      record.maxMs = sorted[sorted.length - 1];
    }
    record.shuffleReadBytes = lastShuffleRead;
    record.shuffleWriteBytes = lastShuffleWrite;
    return record;
  }

  /** Walk the executed physical plan, count operator types and join algorithms. */
  private static void analyzePlan(SparkPlan plan, QueryRecord record) {
    walk(plan, record);
  }

  private static void walk(SparkPlan plan, QueryRecord record) {
    String name = plan.getClass().getSimpleName();
    record.operatorCounts.merge(name, 1, Integer::sum);
    if (name.contains("BroadcastHashJoin")) {
      record.bhjCount++;
    } else if (name.contains("SortMergeJoin")) {
      record.smjCount++;
    } else if (name.contains("ShuffledHashJoin")) {
      record.shjCount++;
    }
    if (name.contains("BatchScan")) {
      String desc = plan.toString();
      if (desc.contains("LanceScan") || desc.contains("Lance")) {
        record.lanceScanCount++;
      }
    }
    scala.collection.Iterator<SparkPlan> it = plan.children().iterator();
    while (it.hasNext()) {
      walk(it.next(), record);
    }
  }

  /** Detect whether any LanceScan reports non-empty {@code colStats=Map(...)} in EXPLAIN COST. */
  private static boolean detectColStats(String explainCost) {
    if (explainCost == null) {
      return false;
    }
    Matcher m = COL_STATS_MAP.matcher(explainCost);
    while (m.find()) {
      String body = m.group(1).trim();
      if (!body.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /** Count Lance scan lines in EXPLAIN COST as a sanity cross-check vs physical plan walk. */
  private static int countLanceScans(String explainCost) {
    if (explainCost == null) {
      return 0;
    }
    int count = 0;
    Matcher m = LANCE_SCAN_LINE.matcher(explainCost);
    while (m.find()) {
      count++;
    }
    return count;
  }

  private static void writeText(Path file, String content) {
    try {
      Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      System.err.println("Failed to write " + file + ": " + e.getMessage());
    }
  }

  private static String[] splitStatements(String sql) {
    return sql.split(";");
  }

  private static String firstNonEmpty(String[] statements) {
    for (String s : statements) {
      String t = s.trim();
      if (!t.isEmpty()) {
        return t;
      }
    }
    return null;
  }

  private static List<String> getAvailableQueries() {
    List<String> queries = new ArrayList<>();
    for (int i = 1; i <= 99; i++) {
      String name = "q" + i;
      if (TpcdsCboMetricsRunner.class.getResourceAsStream("/tpcds-queries/" + name + ".sql") != null) {
        queries.add(name);
      }
      for (String suffix : new String[] {"a", "b"}) {
        String variantName = "q" + i + suffix;
        if (TpcdsCboMetricsRunner.class.getResourceAsStream("/tpcds-queries/" + variantName + ".sql")
            != null) {
          queries.add(variantName);
        }
      }
    }
    return queries;
  }

  private static List<String> filterQueries(List<String> all, String filter) {
    if (filter == null || filter.isEmpty()) {
      return all;
    }
    Set<String> wanted = new HashSet<>(Arrays.asList(filter.split(",")));
    return all.stream().filter(wanted::contains).collect(Collectors.toList());
  }

  private static String loadQuery(String queryName) {
    String resourcePath = "/tpcds-queries/" + queryName + ".sql";
    try (InputStream is = TpcdsCboMetricsRunner.class.getResourceAsStream(resourcePath)) {
      if (is == null) {
        return null;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (Exception e) {
      System.err.println("Failed to load query " + queryName + ": " + e.getMessage());
      return null;
    }
  }

  private static class QueryRecord {
    String queryName;
    String status;
    String errorMessage;
    int iterations;
    long medianMs;
    long minMs;
    long maxMs;
    long shuffleReadBytes;
    long shuffleWriteBytes;
    Map<String, Integer> operatorCounts = new LinkedHashMap<>();
    int bhjCount;
    int smjCount;
    int shjCount;
    int lanceScanCount;
    int lanceScanCountFromExplain;
    boolean colStatsReported;
    int colStatsAttributeCount;
    String explainCostText;
    String physicalPlanText;

    String toCsvRow() {
      return String.join(",",
          queryName,
          status == null ? "" : status,
          String.valueOf(iterations),
          String.valueOf(medianMs),
          String.valueOf(minMs),
          String.valueOf(maxMs),
          String.valueOf(shuffleReadBytes),
          String.valueOf(shuffleWriteBytes),
          String.valueOf(bhjCount),
          String.valueOf(smjCount),
          String.valueOf(shjCount),
          String.valueOf(lanceScanCount),
          colStatsReported ? "Y" : "N");
    }

    String toJson() {
      StringBuilder sb = new StringBuilder();
      sb.append('{');
      jsonField(sb, "queryName", queryName, true);
      jsonField(sb, "status", status, false);
      if (errorMessage != null) {
        jsonField(sb, "errorMessage", errorMessage, false);
      }
      jsonNum(sb, "iterations", iterations);
      jsonNum(sb, "medianMs", medianMs);
      jsonNum(sb, "minMs", minMs);
      jsonNum(sb, "maxMs", maxMs);
      jsonNum(sb, "shuffleReadBytes", shuffleReadBytes);
      jsonNum(sb, "shuffleWriteBytes", shuffleWriteBytes);
      jsonNum(sb, "bhjCount", bhjCount);
      jsonNum(sb, "smjCount", smjCount);
      jsonNum(sb, "shjCount", shjCount);
      jsonNum(sb, "lanceScanCount", lanceScanCount);
      jsonNum(sb, "lanceScanCountFromExplain", lanceScanCountFromExplain);
      sb.append(',').append('"').append("colStatsReported").append('"').append(':')
          .append(colStatsReported ? "true" : "false");
      jsonNum(sb, "colStatsAttributeCount", colStatsAttributeCount);
      sb.append(',').append('"').append("operatorCounts").append('"').append(":{");
      boolean first = true;
      for (Map.Entry<String, Integer> e : operatorCounts.entrySet()) {
        if (!first) {
          sb.append(',');
        }
        sb.append('"').append(jsonEscape(e.getKey())).append("\":").append(e.getValue());
        first = false;
      }
      sb.append('}');
      sb.append('}');
      return sb.toString();
    }
  }

  private static void jsonField(StringBuilder sb, String name, String value, boolean first) {
    if (!first) {
      sb.append(',');
    }
    sb.append('"').append(name).append("\":");
    if (value == null) {
      sb.append("null");
    } else {
      sb.append('"').append(jsonEscape(value)).append('"');
    }
  }

  private static void jsonNum(StringBuilder sb, String name, long value) {
    sb.append(',').append('"').append(name).append("\":").append(value);
  }

  private static String jsonEscape(String s) {
    StringBuilder out = new StringBuilder(s.length() + 8);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"':
          out.append("\\\"");
          break;
        case '\\':
          out.append("\\\\");
          break;
        case '\n':
          out.append("\\n");
          break;
        case '\r':
          out.append("\\r");
          break;
        case '\t':
          out.append("\\t");
          break;
        default:
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    return out.toString();
  }

  private static class Args {
    String dataDir;
    String resultsDir;
    String queryFilter;
    String runLabel;
    int iterations = 1;
    int timeoutSeconds = 300;

    static Args parse(String[] args) {
      Args a = new Args();
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--data-dir":
            a.dataDir = args[++i];
            break;
          case "--results-dir":
            a.resultsDir = args[++i];
            break;
          case "--queries":
            a.queryFilter = args[++i];
            break;
          case "--run-label":
            a.runLabel = args[++i];
            break;
          case "--iterations":
            a.iterations = Integer.parseInt(args[++i]);
            break;
          case "--timeout-seconds":
            a.timeoutSeconds = Integer.parseInt(args[++i]);
            break;
          default:
            System.err.println("Unknown argument: " + args[i]);
            printUsage();
            System.exit(1);
        }
      }
      if (a.dataDir == null || a.resultsDir == null) {
        System.err.println("Missing required arguments.");
        printUsage();
        System.exit(1);
      }
      return a;
    }
  }

  private static void printUsage() {
    System.err.println(
        "Usage: TpcdsCboMetricsRunner"
            + " --data-dir <path>"
            + " --results-dir <path>"
            + " [--queries q1,q3,q19]"
            + " [--iterations 1]"
            + " [--timeout-seconds 300]"
            + " [--run-label baseline-2026-04-29]");
  }
}
