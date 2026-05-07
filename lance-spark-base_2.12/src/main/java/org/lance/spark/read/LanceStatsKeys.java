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
package org.lance.spark.read;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Constants and helpers that define the on-disk wire format of {@code ANALYZE TABLE} persisted
 * column statistics in TBLPROPERTIES. Centralizing these in one base-package utility keeps the
 * ANALYZE writer (in {@code org.apache.spark.sql.execution.datasources.v2}) and the scan-time
 * reader (in {@code org.lance.spark.read}) on the same source of truth without forcing the read
 * path to depend on a class inside Spark's internal package namespace.
 *
 * <h3>Wire format (lance.stats.version = 1)</h3>
 *
 * <ul>
 *   <li>{@code lance.stats.version} = "1" — bumping this invalidates older readers.
 *   <li>{@code lance.stats.complete} = "true" — atomicity sentinel, always written LAST.
 *   <li>{@code lance.stats.computedAtVersion} = decimal Lance manifest version stats reflect.
 *   <li>{@code lance.stats.computedAt} = ISO-8601 instant.
 *   <li>{@code lance.stats.numRows} = decimal row count at ANALYZE time.
 *   <li>{@code lance.stats.schemaHash} = SHA-256 hex of the (name, dataType, nullable) tuple
 *       stream.
 *   <li>{@code lance.stats.column.<name>.{min,max,nullCount,distinctCount,distinctMode,avgLen,
 *       maxLen,histogram,histogramFormat}} — per-column stats. {@code <name>} is a top-level column
 *       name; the format does not currently support nested-leaf paths (see {@link
 *       #validateColumnName} below).
 *   <li>{@code distinctMode} value is the closed set {@link #DISTINCT_MODE_APPROX} / {@link
 *       #DISTINCT_MODE_EXACT}. Today's read path doesn't surface this field to Spark's {@code
 *       ColumnStatistics} (which has no approx/exact flag), but the value is part of the
 *       wire-format contract for future re-ANALYZE tooling that may refresh only approx-NDV
 *       columns.
 *   <li>{@code avgLen} / {@code maxLen} — decimal byte-length stats from Spark's {@code
 *       CommandUtils.computeColumnStats}. Used by Spark's CBO for join-size estimation on
 *       string/binary columns.
 *   <li>{@code histogram} — base64-encoded equi-height histogram body (see {@link
 *       ColumnStatsCodec#encodeHistogram}). Only written when both Spark's {@code
 *       spark.sql.statistics.histogram.enabled} and Lance's {@link
 *       #SPARK_CONF_CBO_COLUMN_STATS_HISTOGRAM_ENABLED} are {@code true}.
 *   <li>{@code histogramFormat} — wire-format tag for the histogram payload ({@link
 *       #HISTOGRAM_FORMAT_V1}). The read path treats unknown tags as fallback signals, not errors,
 *       so older readers gracefully ignore future formats.
 * </ul>
 *
 * <p>The format is a stability contract: existing tags must keep their semantics across connector
 * versions. New stats tags may be added at the same {@code lance.stats.version}; format-breaking
 * changes (existing tag's encoding altered, key prefix renamed) require bumping {@code
 * lance.stats.version} so older readers fall back to live aggregation rather than silently
 * misinterpret the payload.
 *
 * <h3>Design choices and known gaps vs. upstream conventions</h3>
 *
 * <ul>
 *   <li><b>Storage medium:</b> stats live in TBLPROPERTIES on the Lance manifest, not in a Puffin
 *       sidecar (Iceberg's choice) or a separate stats table (Delta's choice). The Lance manifest
 *       is the single source of truth; sidecar files would require a second read path and add a
 *       Puffin dependency. The trade-off is that very large per-column stats (histograms, sketches)
 *       are awkward — but at v1 we only persist scalar bounds and counts, which fit.
 *   <li><b>Per-column only, not table-level:</b> Spark's native {@code ANALYZE TABLE … COMPUTE
 *       STATISTICS} updates both row-count and on-disk-size. This implementation persists row count
 *       under {@code lance.stats.numRows} but the read path takes {@code sizeInBytes} from {@link
 *       org.lance.ManifestSummary} (always live), so plain {@code COMPUTE STATISTICS} without
 *       {@code FOR ALL COLUMNS} is effectively a no-op for the table-size estimate.
 *   <li><b>Output row deviates from native ANALYZE TABLE:</b> Spark's {@code AnalyzeTableCommand}
 *       returns zero rows; this command returns one row of {@code (columns_analyzed, num_rows,
 *       manifest_version, approx)} for tooling and operator diagnostics.
 * </ul>
 */
public final class LanceStatsKeys {

  /** Top-level table-property keys. */
  public static final String VERSION = "lance.stats.version";

  public static final String COMPLETE = "lance.stats.complete";
  public static final String COMPUTED_AT_VERSION = "lance.stats.computedAtVersion";
  public static final String COMPUTED_AT = "lance.stats.computedAt";
  public static final String NUM_ROWS = "lance.stats.numRows";
  public static final String SCHEMA_HASH = "lance.stats.schemaHash";

  /** Per-column key prefix; full key = {@code COLUMN_PREFIX + name + COLUMN_SUFFIX_*}. */
  public static final String COLUMN_PREFIX = "lance.stats.column.";

  public static final String COLUMN_SUFFIX_MIN = ".min";
  public static final String COLUMN_SUFFIX_MAX = ".max";
  public static final String COLUMN_SUFFIX_NULL_COUNT = ".nullCount";
  public static final String COLUMN_SUFFIX_DISTINCT_COUNT = ".distinctCount";
  public static final String COLUMN_SUFFIX_DISTINCT_MODE = ".distinctMode";
  public static final String COLUMN_SUFFIX_AVG_LEN = ".avgLen";
  public static final String COLUMN_SUFFIX_MAX_LEN = ".maxLen";
  public static final String COLUMN_SUFFIX_HISTOGRAM = ".histogram";
  public static final String COLUMN_SUFFIX_HISTOGRAM_FORMAT = ".histogramFormat";

  /**
   * Wire-format tag for the {@code .histogram} payload. Bumped only on a format-breaking change to
   * {@link ColumnStatsCodec#encodeHistogram}; older readers see an unknown tag and silently drop
   * the histogram (other stats still load).
   */
  public static final String HISTOGRAM_FORMAT_V1 = "v1";

  /**
   * Legal values for {@code lance.stats.column.<name>.distinctMode}. {@code "approx"} indicates the
   * NDV came from {@code approx_count_distinct} (HLL); {@code "exact"} from {@code
   * COUNT(DISTINCT)}. The value set is part of the wire-format stability contract — readers may
   * parse this key in future tooling (e.g., a re-ANALYZE that only refreshes approx-NDV columns),
   * so the strings cannot change without bumping {@link #SUPPORTED_VERSION}.
   */
  public static final String DISTINCT_MODE_APPROX = "approx";

  public static final String DISTINCT_MODE_EXACT = "exact";

  /** Format version this connector knows how to read and write. */
  public static final String SUPPORTED_VERSION = "1";

  // SparkConf keys below are part of the operator-facing configuration contract — once a
  // release ships, renaming requires a deprecation cycle. All share the prefix
  // `spark.lance.cbo.column.stats.` for namespace introspection.

  /**
   * Master kill-switch for the column-stats fast path. Overrides per-scan {@code
   * lance.cbo.column.stats.enabled}. Default: {@code true}.
   */
  public static final String SPARK_CONF_CBO_COLUMN_STATS_ENABLED =
      "spark.lance.cbo.column.stats.enabled";

  /**
   * Cap on number of columns the read path loads zonemap stats for at plan time. Overrides per-scan
   * {@code lance.cbo.column.stats.max.columns}. Default: {@code 8} (see {@code
   * LanceSparkReadOptions.DEFAULT_CBO_COLUMN_STATS_MAX_COLUMNS}).
   */
  public static final String SPARK_CONF_CBO_COLUMN_STATS_MAX_COLUMNS =
      "spark.lance.cbo.column.stats.max.columns";

  /**
   * If {@code true}, accept persisted stats whose {@code computedAtVersion} differs from the
   * current Lance manifest version. Default: {@code false}. Trades CBO correctness for fast-path
   * hit rate; only enable when stale estimates are acceptable.
   */
  public static final String SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE =
      "spark.lance.cbo.column.stats.allow.stale";

  /**
   * Lance-local kill-switch for histogram persistence. Histograms are only computed AND persisted
   * when BOTH this flag and Spark's global {@code spark.sql.statistics.histogram.enabled} are
   * {@code true}. Default: {@code false}, so the refactor is a no-op on histograms until operators
   * opt in. Decoupled from Spark's global flag so a cluster-wide flip for HMS-backed tables doesn't
   * bloat Lance manifests.
   */
  public static final String SPARK_CONF_CBO_COLUMN_STATS_HISTOGRAM_ENABLED =
      "spark.lance.cbo.column.stats.histogram.enabled";

  /** Default for {@link #SPARK_CONF_CBO_COLUMN_STATS_HISTOGRAM_ENABLED}. */
  public static final boolean DEFAULT_CBO_COLUMN_STATS_HISTOGRAM_ENABLED = false;

  /**
   * Cache the table relation between the {@code CommandUtils.computeColumnStats} pass and the
   * exact-NDV pass under {@code approx=false}. {@code true} (default) avoids the second physical
   * scan by reusing Spark's {@code InMemoryRelation} substitution; {@code false} skips the cache
   * for memory-constrained clusters, accepting one extra scan per ANALYZE.
   */
  public static final String SPARK_CONF_CBO_COLUMN_STATS_CACHE_ENABLED =
      "spark.lance.cbo.column.stats.cache.enabled";

  /** Default for {@link #SPARK_CONF_CBO_COLUMN_STATS_CACHE_ENABLED}. */
  public static final boolean DEFAULT_CBO_COLUMN_STATS_CACHE_ENABLED = true;

  /**
   * Regex capturing the column name segment between {@link #COLUMN_PREFIX} and a known suffix.
   * Anchored on the closed suffix set so the captured name can legitimately contain dots if a
   * future writer ever persists per-leaf stats for nested fields. Today's writer rejects dotted
   * column names (see {@link #validateColumnName}), so any dotted capture is by construction an
   * orphan and is safe to remove during ANALYZE's GC pass.
   */
  public static final String COLUMN_KEY_REGEX =
      "^lance\\.stats\\.column\\.(.*)\\.(?:min|max|nullCount|distinctCount|distinctMode"
          + "|avgLen|maxLen|histogram|histogramFormat)$";

  private LanceStatsKeys() {}

  /** Maximum length of a column name surfaced in error messages, to bound exception size. */
  private static final int MAX_NAME_IN_ERROR = 256;

  /**
   * True when {@code name} produces a non-ambiguous TBLPROPERTIES key under {@link #COLUMN_PREFIX}.
   * Ambiguity sources today: dots in the name (collide with future nested-leaf paths) and the empty
   * string (would emit a key like {@code lance.stats.column..min} that orphan-cleanup would later
   * misattribute).
   */
  public static boolean isStatsCompatibleColumnName(String name) {
    return name != null && !name.isEmpty() && name.indexOf('.') < 0;
  }

  /**
   * Reject column names that would produce ambiguous TBLPROPERTIES keys: dotted names collide with
   * future nested-leaf paths, empty names collide with the orphan-cleanup regex. Forbidding at
   * write time is more conservative than introducing wire-format escaping.
   *
   * @throws IllegalArgumentException if the name is null, empty, or contains a dot.
   */
  public static void validateColumnName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Column name must not be null");
    }
    if (name.isEmpty()) {
      throw new IllegalArgumentException(
          "ANALYZE TABLE does not support empty column names. Persisted-stats keys would "
              + "collide with the orphan-cleanup regex.");
    }
    if (!isStatsCompatibleColumnName(name)) {
      // Cap the echoed name in the message to bound exception size — a hostile or accidental
      // 10MB column name should not produce a 10MB error string that flows back to the driver.
      String shown =
          name.length() > MAX_NAME_IN_ERROR ? name.substring(0, MAX_NAME_IN_ERROR) + "…" : name;
      throw new IllegalArgumentException(
          "ANALYZE TABLE does not support column names containing '.': '"
              + shown
              + "'. Persisted-stats keys would be ambiguous with future nested-leaf paths. "
              + "Rename the column or omit it from FOR COLUMNS.");
    }
  }

  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

  /**
   * Per-thread {@link MessageDigest} so {@link #computeSchemaHash} avoids the JCE provider lookup
   * on every call; reset() before reuse keeps it thread-safe.
   */
  private static final ThreadLocal<MessageDigest> SHA256 =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
              throw new IllegalStateException("SHA-256 not available", e);
            }
          });

  /**
   * SHA-256 fingerprint of the schema's "field name + data-type + nullable" triples in declared
   * order. Lets the read path detect schema drift between ANALYZE and a subsequent scan: if any
   * column was renamed, retyped, had its nullability flipped, dropped, or reordered in a way that
   * changes resolution, the hash diverges and persisted stats are rejected.
   *
   * <p><b>Schema-mutation monotonicity (intentional):</b> the hash covers ALL fields, so adding,
   * dropping, retyping, reordering, or flipping nullability of any single column invalidates the
   * persisted stats for ALL columns — not just the one that changed. This is the safe direction:
   * the read path falls back to live aggregation, never reports stats it cannot prove still apply.
   * Operators must re-run ANALYZE TABLE after any {@code ALTER TABLE} that changes the schema, even
   * if only one column was touched.
   *
   * <p>Stable across JVM instances because we hash a deterministic UTF-8 byte stream rather than
   * relying on Scala or Java's hashCode (which is salted in some implementations).
   */
  public static String computeSchemaHash(StructType schema) {
    MessageDigest md = SHA256.get();
    md.reset();
    for (StructField f : schema.fields()) {
      md.update(f.name().getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(f.dataType().catalogString().getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(f.nullable() ? (byte) 1 : (byte) 0);
      md.update((byte) 0);
    }
    byte[] digest = md.digest();
    char[] hex = new char[digest.length * 2];
    for (int i = 0; i < digest.length; i++) {
      int b = digest[i] & 0xff;
      hex[i * 2] = HEX_CHARS[b >>> 4];
      hex[i * 2 + 1] = HEX_CHARS[b & 0x0f];
    }
    return new String(hex);
  }
}
