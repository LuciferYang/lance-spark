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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregates per-query JSONL captured by {@link TpcdsCboMetricsRunner} into a Markdown report.
 *
 * <p>Single-run mode (one input directory): summarizes one capture — pass rate, plan-shape
 * distribution, BHJ/SMJ/SHJ totals, column-stats coverage, geomean runtime.
 *
 * <p>A/B mode (two input directories: {@code --before} and {@code --after}): adds a per-query diff
 * — plans changed, SMJ→BHJ flips, runtime delta, shuffle delta, regressions — corresponding to
 * metrics M3, M4, M6, M7, M8 in {@code cbo-enablement-plan.md}.
 */
public class CboMetricsReporter {

  public static void main(String[] args) throws Exception {
    String beforeDir = null;
    String afterDir = null;
    String singleDir = null;
    String outFile = null;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--before":
          beforeDir = args[++i];
          break;
        case "--after":
          afterDir = args[++i];
          break;
        case "--run":
          singleDir = args[++i];
          break;
        case "--out":
          outFile = args[++i];
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (singleDir != null) {
      Path runDir = Paths.get(singleDir);
      List<Record> records = loadJsonl(runDir.resolve("runs.jsonl"));
      String report = singleRunReport(runDir.getFileName().toString(), records);
      writeOrPrint(outFile != null ? outFile : runDir.resolve("report.md").toString(), report);
    } else if (beforeDir != null && afterDir != null) {
      Path beforePath = Paths.get(beforeDir);
      Path afterPath = Paths.get(afterDir);
      List<Record> before = loadJsonl(beforePath.resolve("runs.jsonl"));
      List<Record> after = loadJsonl(afterPath.resolve("runs.jsonl"));
      String report = compareReport(
          beforePath.getFileName().toString(),
          afterPath.getFileName().toString(),
          before, after);
      writeOrPrint(outFile != null ? outFile
          : afterPath.resolve("compare-with-" + beforePath.getFileName() + ".md").toString(),
          report);
    } else {
      printUsage();
      System.exit(1);
    }
  }

  private static String singleRunReport(String runLabel, List<Record> records) {
    StringBuilder sb = new StringBuilder();
    sb.append("# CBO Metrics — ").append(runLabel).append("\n\n");

    int total = records.size();
    long passed = records.stream().filter(r -> "OK".equals(r.status)).count();
    long failed = total - passed;

    long bhjTotal = records.stream().mapToLong(r -> r.bhjCount).sum();
    long smjTotal = records.stream().mapToLong(r -> r.smjCount).sum();
    long shjTotal = records.stream().mapToLong(r -> r.shjCount).sum();
    long lanceScanTotal = records.stream().mapToLong(r -> r.lanceScanCount).sum();
    long colStatsCount = records.stream().filter(r -> r.colStatsReported).count();
    long shuffleReadTotal = records.stream().mapToLong(r -> r.shuffleReadBytes).sum();

    double geomeanMs = geomean(records.stream()
        .filter(r -> "OK".equals(r.status) && r.medianMs > 0)
        .mapToLong(r -> r.medianMs).toArray());

    sb.append("## Summary\n\n");
    sb.append("| Metric | Value |\n|---|---|\n");
    sb.append("| Queries run | ").append(total).append(" |\n");
    sb.append("| Passed (M9) | ").append(passed).append("/").append(total).append(" |\n");
    sb.append("| Failed | ").append(failed).append(" |\n");
    sb.append("| Total BroadcastHashJoin nodes | ").append(bhjTotal).append(" |\n");
    sb.append("| Total SortMergeJoin nodes (M4 baseline) | ").append(smjTotal).append(" |\n");
    sb.append("| Total ShuffledHashJoin nodes | ").append(shjTotal).append(" |\n");
    sb.append("| Total LanceScan operators | ").append(lanceScanTotal).append(" |\n");
    sb.append("| Queries with colStats reported (M1) | ").append(colStatsCount)
        .append("/").append(total).append(" |\n");
    sb.append("| Geomean runtime ms (passing only, M6 baseline) | ")
        .append(String.format("%.0f", geomeanMs)).append(" |\n");
    sb.append("| Total shuffle read bytes (M7 baseline) | ")
        .append(formatBytes(shuffleReadTotal)).append(" |\n\n");

    sb.append("## Per-query plan signature\n\n");
    sb.append("| Query | Status | Median ms | BHJ | SMJ | SHJ | Lance scans | colStats |\n");
    sb.append("|---|---|---:|---:|---:|---:|---:|:---:|\n");
    records.stream()
        .sorted(Comparator.comparing(r -> r.queryName))
        .forEach(r -> sb.append("| ").append(r.queryName)
            .append(" | ").append(r.status == null ? "?" : r.status)
            .append(" | ").append("OK".equals(r.status) ? Long.toString(r.medianMs) : "—")
            .append(" | ").append(r.bhjCount)
            .append(" | ").append(r.smjCount)
            .append(" | ").append(r.shjCount)
            .append(" | ").append(r.lanceScanCount)
            .append(" | ").append(r.colStatsReported ? "Y" : "N")
            .append(" |\n"));

    sb.append("\n## Failures\n\n");
    List<Record> fails = new ArrayList<>();
    records.stream().filter(r -> !"OK".equals(r.status)).forEach(fails::add);
    if (fails.isEmpty()) {
      sb.append("None.\n");
    } else {
      sb.append("| Query | Error |\n|---|---|\n");
      for (Record r : fails) {
        sb.append("| ").append(r.queryName).append(" | ")
            .append(r.errorMessage == null ? "(no message)" : truncate(r.errorMessage, 200))
            .append(" |\n");
      }
    }

    return sb.toString();
  }

  private static String compareReport(String beforeLabel, String afterLabel,
      List<Record> before, List<Record> after) {
    Map<String, Record> beforeMap = new HashMap<>();
    for (Record r : before) {
      beforeMap.put(r.queryName, r);
    }
    Map<String, Record> afterMap = new HashMap<>();
    for (Record r : after) {
      afterMap.put(r.queryName, r);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("# CBO Metrics A/B — ").append(beforeLabel).append(" vs ").append(afterLabel)
        .append("\n\n");

    int plansChanged = 0;
    int smjToBhjFlips = 0;
    int regressions20pct = 0;
    long shuffleBefore = 0;
    long shuffleAfter = 0;
    List<Long> beforeTimes = new ArrayList<>();
    List<Long> afterTimes = new ArrayList<>();

    List<String> queries = new ArrayList<>(beforeMap.keySet());
    queries.retainAll(afterMap.keySet());
    queries.sort(Comparator.naturalOrder());

    StringBuilder perQuery = new StringBuilder();
    perQuery.append("## Per-query diff\n\n");
    perQuery.append("| Query | Plan changed | BHJ Δ | SMJ Δ | colStats Δ | "
        + "Median Δ | Median % | Note |\n");
    perQuery.append("|---|:---:|---:|---:|:---:|---:|---:|---|\n");

    for (String q : queries) {
      Record b = beforeMap.get(q);
      Record a = afterMap.get(q);
      boolean planChanged = b.bhjCount != a.bhjCount || b.smjCount != a.smjCount
          || b.shjCount != a.shjCount;
      if (planChanged) {
        plansChanged++;
      }
      int bhjDelta = a.bhjCount - b.bhjCount;
      int smjDelta = a.smjCount - b.smjCount;
      if (smjDelta < 0 && bhjDelta > 0) {
        smjToBhjFlips++;
      }
      String colStatsDelta;
      if (b.colStatsReported == a.colStatsReported) {
        colStatsDelta = "=";
      } else if (a.colStatsReported) {
        colStatsDelta = "+";
      } else {
        colStatsDelta = "-";
      }
      long medianDelta = a.medianMs - b.medianMs;
      double pct = b.medianMs > 0 ? (double) medianDelta * 100.0 / b.medianMs : 0.0;
      String note = "";
      boolean bothPass = "OK".equals(b.status) && "OK".equals(a.status);
      if (bothPass) {
        beforeTimes.add(b.medianMs);
        afterTimes.add(a.medianMs);
        shuffleBefore += b.shuffleReadBytes;
        shuffleAfter += a.shuffleReadBytes;
        if (pct > 20.0) {
          regressions20pct++;
          note = "**REGRESSION**";
        }
      } else if (!"OK".equals(b.status) && "OK".equals(a.status)) {
        note = "fixed";
      } else if ("OK".equals(b.status) && !"OK".equals(a.status)) {
        note = "**BROKE**";
      }

      perQuery.append("| ").append(q)
          .append(" | ").append(planChanged ? "Y" : "")
          .append(" | ").append(formatSigned(bhjDelta))
          .append(" | ").append(formatSigned(smjDelta))
          .append(" | ").append(colStatsDelta)
          .append(" | ").append(bothPass ? Long.toString(medianDelta) : "—")
          .append(" | ").append(bothPass ? String.format("%+.1f%%", pct) : "—")
          .append(" | ").append(note)
          .append(" |\n");
    }

    double geomeanBefore = geomean(toLongArray(beforeTimes));
    double geomeanAfter = geomean(toLongArray(afterTimes));
    double geomeanPct = geomeanBefore > 0
        ? (geomeanAfter - geomeanBefore) * 100.0 / geomeanBefore : 0.0;

    sb.append("## Aggregate metrics\n\n");
    sb.append("| Metric | Before | After | Delta | Threshold |\n|---|---:|---:|---:|---|\n");
    sb.append("| M1: queries with colStats | ")
        .append(before.stream().filter(r -> r.colStatsReported).count())
        .append(" | ")
        .append(after.stream().filter(r -> r.colStatsReported).count())
        .append(" | — | ≥60% of total |\n");
    sb.append("| M3: queries with plan change | — | ").append(plansChanged)
        .append(" | — | ≥8 |\n");
    sb.append("| M4: SMJ→BHJ flips | — | ").append(smjToBhjFlips).append(" | — | ≥4 |\n");
    sb.append("| M6: geomean runtime ms | ")
        .append(String.format("%.0f", geomeanBefore)).append(" | ")
        .append(String.format("%.0f", geomeanAfter)).append(" | ")
        .append(String.format("%+.1f%%", geomeanPct)).append(" | ≤−5% |\n");
    sb.append("| M7: total shuffle read | ").append(formatBytes(shuffleBefore))
        .append(" | ").append(formatBytes(shuffleAfter))
        .append(" | ").append(shuffleBefore > 0
            ? String.format("%+.1f%%",
                (shuffleAfter - shuffleBefore) * 100.0 / shuffleBefore) : "—")
        .append(" | ≤−10% |\n");
    sb.append("| M8: queries with >20% slowdown | — | ").append(regressions20pct)
        .append(" | — | ≤2 |\n");
    long beforePass = before.stream().filter(r -> "OK".equals(r.status)).count();
    long afterPass = after.stream().filter(r -> "OK".equals(r.status)).count();
    sb.append("| M9: pass count | ").append(beforePass).append("/").append(before.size())
        .append(" | ").append(afterPass).append("/").append(after.size())
        .append(" | — | After ≥ Before |\n\n");

    sb.append(perQuery);
    return sb.toString();
  }

  private static List<Record> loadJsonl(Path file) throws IOException {
    List<Record> out = new ArrayList<>();
    try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line;
      while ((line = r.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
        out.add(Record.fromJson(line));
      }
    }
    return out;
  }

  private static double geomean(long[] values) {
    if (values.length == 0) {
      return 0.0;
    }
    double sumLog = 0.0;
    int n = 0;
    for (long v : values) {
      if (v > 0) {
        sumLog += Math.log(v);
        n++;
      }
    }
    if (n == 0) {
      return 0.0;
    }
    return Math.exp(sumLog / n);
  }

  private static long[] toLongArray(List<Long> list) {
    long[] out = new long[list.size()];
    for (int i = 0; i < list.size(); i++) {
      out[i] = list.get(i);
    }
    return out;
  }

  private static String formatSigned(int v) {
    return v == 0 ? "0" : (v > 0 ? "+" + v : Integer.toString(v));
  }

  private static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + "B";
    }
    if (bytes < 1024 * 1024) {
      return String.format("%.0fKB", bytes / 1024.0);
    }
    if (bytes < 1024L * 1024 * 1024) {
      return String.format("%.0fMB", bytes / (1024.0 * 1024));
    }
    return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
  }

  private static String truncate(String s, int max) {
    return s.length() <= max ? s : s.substring(0, max) + "...";
  }

  private static void writeOrPrint(String dest, String content) throws IOException {
    if ("-".equals(dest)) {
      System.out.print(content);
    } else {
      Path p = Paths.get(dest);
      if (p.getParent() != null) {
        Files.createDirectories(p.getParent());
      }
      Files.write(p, content.getBytes(StandardCharsets.UTF_8));
      System.out.println("Wrote " + dest);
    }
  }

  private static void printUsage() {
    System.err.println(
        "Usage: CboMetricsReporter "
            + "(--run <dir> | --before <dir> --after <dir>) [--out <md-file>]\n"
            + "  Single-run mode:  --run <run-dir>\n"
            + "  A/B compare mode: --before <baseline-dir> --after <phase1-dir>\n"
            + "  --out -            print to stdout\n"
            + "  --out <file>       write to file (default: <run-dir>/report.md)");
  }

  /** Minimal JSON record parser tied to the keys emitted by {@link TpcdsCboMetricsRunner}. */
  private static class Record {
    String queryName;
    String status;
    String errorMessage;
    long medianMs;
    long minMs;
    long maxMs;
    long shuffleReadBytes;
    long shuffleWriteBytes;
    int bhjCount;
    int smjCount;
    int shjCount;
    int lanceScanCount;
    boolean colStatsReported;

    static Record fromJson(String json) {
      Record r = new Record();
      r.queryName = readString(json, "queryName");
      r.status = readString(json, "status");
      r.errorMessage = readString(json, "errorMessage");
      r.medianMs = readLong(json, "medianMs");
      r.minMs = readLong(json, "minMs");
      r.maxMs = readLong(json, "maxMs");
      r.shuffleReadBytes = readLong(json, "shuffleReadBytes");
      r.shuffleWriteBytes = readLong(json, "shuffleWriteBytes");
      r.bhjCount = (int) readLong(json, "bhjCount");
      r.smjCount = (int) readLong(json, "smjCount");
      r.shjCount = (int) readLong(json, "shjCount");
      r.lanceScanCount = (int) readLong(json, "lanceScanCount");
      r.colStatsReported = readBool(json, "colStatsReported");
      return r;
    }

    private static String readString(String json, String key) {
      int idx = json.indexOf("\"" + key + "\":");
      if (idx < 0) {
        return null;
      }
      int valueStart = idx + key.length() + 3;
      while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
        valueStart++;
      }
      if (valueStart >= json.length()) {
        return null;
      }
      if (json.charAt(valueStart) == 'n') {
        return null;
      }
      if (json.charAt(valueStart) != '"') {
        return null;
      }
      StringBuilder sb = new StringBuilder();
      int i = valueStart + 1;
      while (i < json.length()) {
        char c = json.charAt(i);
        if (c == '\\' && i + 1 < json.length()) {
          char next = json.charAt(i + 1);
          switch (next) {
            case '"':
              sb.append('"');
              break;
            case '\\':
              sb.append('\\');
              break;
            case 'n':
              sb.append('\n');
              break;
            case 'r':
              sb.append('\r');
              break;
            case 't':
              sb.append('\t');
              break;
            default:
              sb.append(next);
          }
          i += 2;
        } else if (c == '"') {
          break;
        } else {
          sb.append(c);
          i++;
        }
      }
      return sb.toString();
    }

    private static long readLong(String json, String key) {
      int idx = json.indexOf("\"" + key + "\":");
      if (idx < 0) {
        return 0L;
      }
      int valueStart = idx + key.length() + 3;
      while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
        valueStart++;
      }
      int end = valueStart;
      while (end < json.length()) {
        char c = json.charAt(end);
        if (c == ',' || c == '}' || Character.isWhitespace(c)) {
          break;
        }
        end++;
      }
      String raw = json.substring(valueStart, end);
      try {
        return Long.parseLong(raw);
      } catch (NumberFormatException e) {
        return 0L;
      }
    }

    private static boolean readBool(String json, String key) {
      int idx = json.indexOf("\"" + key + "\":");
      if (idx < 0) {
        return false;
      }
      return json.startsWith("true", idx + key.length() + 3);
    }
  }
}
