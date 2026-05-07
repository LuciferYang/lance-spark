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

import org.apache.spark.sql.catalyst.plans.logical.Histogram;
import org.apache.spark.sql.catalyst.plans.logical.HistogramBin;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Encode / decode typed values for {@code lance.stats.column.<name>.{min,max}} table properties.
 * String-typed properties survive Lance's manifest serialization; the encoded form is {@code
 * "<typeTag>:<base64-or-literal>"} so the decoder can recover the original type without out-of-band
 * schema lookup.
 *
 * <h3>Wire-format spec (lance.stats.version = 1) — STABILITY CONTRACT</h3>
 *
 * <p>This codec defines the on-disk encoding of column-statistics min/max values inside Lance
 * manifest TBLPROPERTIES. The format is a stability contract: existing tags must keep their
 * encoding across connector versions so a table analyzed by an older connector remains readable by
 * newer connectors. Format-breaking changes require a {@code lance.stats.version} bump.
 *
 * <p>Tag set is closed under v1. Only additive changes are permitted at v1:
 *
 * <ul>
 *   <li>Adding a NEW tag for a previously unsupported type is allowed and stays at v1 — older
 *       readers get {@code null} from {@link #decode} (unknown tag) and fall back to live
 *       aggregation.
 *   <li>Changing the encoding of an EXISTING tag (re-interpretation, byte-order change, base
 *       reformat, etc.) is forbidden at v1. Any such change must bump {@code lance.stats.version}
 *       to {@code "2"} so older readers reject the payload up front.
 *   <li>Removing a tag is forbidden at v1 — even if no current writer emits it, an old table on
 *       disk may contain it. Removal requires a version bump.
 * </ul>
 *
 * <p>Encoding strategy per type:
 *
 * <ul>
 *   <li>Numeric primitives (Integer, Long, Float, Double, Short, Byte): plain decimal string under
 *       tags {@code i32, i64, f32, f64, i16, i8}.
 *   <li>Boolean: {@code "bool:true"} / {@code "bool:false"}.
 *   <li>String: {@code "utf8:<base64>"} — base64 to survive embedded delimiters and binary text.
 *   <li>Binary: {@code "bin:<base64>"}.
 *   <li>Date (days since epoch as Integer): {@code "date:<int>"}.
 *   <li>Timestamp with timezone (microseconds as Long): {@code "ts:<long>"}.
 *   <li>TimestampNTZ (microseconds-since-epoch UTC as Long): {@code "tsntz:<long>"}.
 *   <li>Decimal: {@code "dec:<scale>:<unscaled>"}.
 * </ul>
 *
 * <p><b>Round-trip exception (NaN / Infinity):</b> for {@code FloatType} / {@code DoubleType},
 * non-finite values ({@code NaN}, {@code +Inf}, {@code -Inf}) are rejected on BOTH sides: {@link
 * #encode} returns {@code null} so the writer never persists a non-finite bound, and {@link
 * #decode} also returns {@code null} for a poisoned {@code f32:NaN} / {@code f64:Infinity}
 * wire-form (e.g. an externally-edited TBLPROPERTIES). The decode-side guard is independent —
 * external tools that bypass {@code encode} cannot smuggle non-finite values into the read path.
 * This intentionally breaks the {@code decode(encode(x)) == x} invariant for those values: a NaN
 * bound mis-orders against any finite value and would make Spark's CBO compute empty ranges,
 * pruning rows that actually satisfy the predicate. The safe direction is to skip the stat. This is
 * the only case where {@code encode} returns {@code null} for a non-null input of a supported
 * scalar type.
 */
public final class ColumnStatsCodec {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnStatsCodec.class);

  /**
   * Hard cap on a single base64 body before we will decode it. Min/max stats for a string column
   * have no legitimate reason to exceed a few KB; refusing to decode beyond this protects the
   * driver against a poisoned or corrupted property triggering OOM via giant base64 payload. 64 KB
   * of base64-encoded text decodes to 48 KB of raw bytes, which is far above any sane stats
   * payload.
   */
  private static final int MAX_BASE64_BODY_LEN = 65_536;

  /**
   * Hard cap on the unscaled-integer body of a {@code dec:} encoded value. Spark's {@code
   * DecimalType} maxes out at precision 38, so the unscaled-value string is at most 39 characters
   * (sign + 38 digits). 64 is a comfortable safety margin while still bounding {@link
   * java.math.BigInteger}'s O(n^2) decimal-string parse cost — a poisoned property containing a
   * multi-million-digit body could otherwise pin a driver core during scan planning.
   */
  private static final int MAX_DECIMAL_BODY_LEN = 64;

  /**
   * Hard cap on the tag (the substring before the first {@code ':'}) before any switch dispatch.
   * Known tags are at most 5 characters ({@code tsntz}); 32 is a comfortable safety margin while
   * preventing a poisoned property like {@code "<10MB-of-A>:body"} from sitting on the heap or
   * flowing into a debug log.
   */
  private static final int MAX_TAG_LEN = 32;

  /**
   * Hard cap on the body of any numeric / timestamp / date / boolean tag. Legitimate inputs are
   * bounded: longs are ≤20 chars (-9223372036854775808), doubles ≤24 chars in scientific notation,
   * {@code bool:true|false} ≤5 chars. 64 is a comfortable safety margin while preventing a poisoned
   * property like {@code f32:<10MB-of-9>} from pinning the driver thread inside {@code
   * Float.parseFloat}'s O(n) scan.
   */
  private static final int MAX_NUMERIC_BODY_LEN = 64;

  private ColumnStatsCodec() {}

  /**
   * Encode a Spark-row value (post-collect, JVM type) for a given column type. Returns {@code null}
   * when:
   *
   * <ul>
   *   <li>{@code value} is null.
   *   <li>{@code dataType} is unsupported by this codec (caller should skip writing the stat).
   *   <li>{@code value} has an unexpected runtime type for the declared {@code dataType} (e.g. a
   *       {@code String} passed for {@code BinaryType}).
   *   <li>{@code dataType} is float/double and {@code value} is NaN or Infinity (see class doc).
   * </ul>
   */
  public static String encode(Object value, DataType dataType) {
    if (value == null) {
      return null;
    }
    if (dataType instanceof IntegerType) {
      return "i32:" + value.toString();
    }
    if (dataType instanceof LongType) {
      return "i64:" + value.toString();
    }
    if (dataType instanceof ShortType) {
      return "i16:" + value.toString();
    }
    if (dataType instanceof ByteType) {
      return "i8:" + value.toString();
    }
    if (dataType instanceof FloatType) {
      // Skip non-Float runtime types and non-finite values. Spark's MIN/MAX over a NaN-bearing
      // column returns NaN, which would mis-order against finite values and make CBO compute
      // empty predicate ranges. See the class doc's "Round-trip exception" section.
      if (!(value instanceof Float)) {
        return null;
      }
      Float f = (Float) value;
      if (f.isNaN() || f.isInfinite()) {
        return null;
      }
      return "f32:" + f;
    }
    if (dataType instanceof DoubleType) {
      if (!(value instanceof Double)) {
        return null;
      }
      Double d = (Double) value;
      if (d.isNaN() || d.isInfinite()) {
        return null;
      }
      return "f64:" + d;
    }
    if (dataType instanceof BooleanType) {
      return "bool:" + value.toString();
    }
    if (dataType instanceof StringType) {
      byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
      return "utf8:" + Base64.getEncoder().encodeToString(bytes);
    }
    if (dataType instanceof BinaryType) {
      // Spark's Row#get for BinaryType returns byte[], but defend against any caller path that
      // ever hands us an InternalRow-derived type or a wrapper. Returning null on a type
      // mismatch matches the codec's "null on unsupported" contract; throwing ClassCastException
      // would crash ANALYZE TABLE rather than degrade gracefully.
      if (!(value instanceof byte[])) {
        return null;
      }
      return "bin:" + Base64.getEncoder().encodeToString((byte[]) value);
    }
    if (dataType instanceof DateType) {
      // Spark's Row#get for DateType returns either java.sql.Date (default) or
      // java.time.LocalDate (when spark.sql.datetime.java8API.enabled=true). Both encode to
      // the same days-since-epoch wire form. Return null for any other runtime type rather
      // than falling through to an opaque toString() that the decoder cannot parse.
      if (value instanceof java.sql.Date) {
        return "date:" + ((java.sql.Date) value).toLocalDate().toEpochDay();
      }
      if (value instanceof java.time.LocalDate) {
        return "date:" + ((java.time.LocalDate) value).toEpochDay();
      }
      return null;
    }
    if (dataType instanceof TimestampType) {
      // Same pattern as DateType: java.sql.Timestamp by default, java.time.Instant under
      // spark.sql.datetime.java8API.enabled=true. Both encode to micros-since-epoch.
      if (value instanceof java.sql.Timestamp) {
        java.sql.Timestamp ts = (java.sql.Timestamp) value;
        long micros = ts.getTime() * 1000L + (ts.getNanos() % 1_000_000) / 1000L;
        return "ts:" + micros;
      }
      if (value instanceof java.time.Instant) {
        java.time.Instant ins = (java.time.Instant) value;
        long micros = ins.getEpochSecond() * 1_000_000L + ins.getNano() / 1000L;
        return "ts:" + micros;
      }
      return null;
    }
    if (dataType instanceof TimestampNTZType) {
      // Spark's Row#get for TimestampNTZType returns java.time.LocalDateTime. Encode as
      // microseconds-since-epoch interpreted at UTC — same wire format as `ts:` so the read
      // path can reuse the Long it produces; CatalystTypeConverters for TimestampNTZType
      // accepts a Long (microseconds-since-epoch UTC) and converts to LocalDateTime internally.
      if (value instanceof java.time.LocalDateTime) {
        java.time.LocalDateTime ldt = (java.time.LocalDateTime) value;
        long epochSecond = ldt.toEpochSecond(java.time.ZoneOffset.UTC);
        long micros = epochSecond * 1_000_000L + ldt.getNano() / 1000L;
        return "tsntz:" + micros;
      }
      // Any other runtime type — return null per the codec contract rather than falling
      // through to value.toString(), which produces a string the decoder cannot parse.
      return null;
    }
    if (dataType instanceof DecimalType) {
      BigDecimal bd =
          value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
      return "dec:" + bd.scale() + ":" + bd.unscaledValue();
    }
    // Unsupported type — caller should skip writing the stat for this column.
    return null;
  }

  /**
   * Decode an encoded property back to a JVM object compatible with Spark's {@code ColumnStat}.
   * Returns {@code null} for malformed, oversized, or unrecognized payloads — caller (the read
   * path) treats {@code null} as "stat unavailable" and falls back to live aggregation.
   *
   * <p>Each null-return path emits a DEBUG log so an operator with debug logging on can trace why a
   * stat went missing without instrumenting the codec itself. INFO level would create unbounded log
   * volume on poisoned-property scans.
   */
  public static Object decode(String encoded) {
    if (encoded == null) {
      return null;
    }
    int sep = encoded.indexOf(':');
    if (sep < 0) {
      LOG.debug("decode: encoded value missing ':' delimiter (length={})", encoded.length());
      return null;
    }
    if (sep > MAX_TAG_LEN) {
      // Bound the tag substring before allocating it: a poisoned property like
      // "<huge-string>:body" would otherwise materialize the giant prefix on the heap and
      // flow into the unrecognized-tag debug log.
      LOG.debug("decode: tag length {} exceeds MAX_TAG_LEN={}", sep, MAX_TAG_LEN);
      return null;
    }
    String tag = encoded.substring(0, sep);
    String body = encoded.substring(sep + 1);
    try {
      return decodeBody(tag, body);
    } catch (NumberFormatException e) {
      // Numeric arms (i32/i64/i16/i8/f32/f64/ts/tsntz/date) parse the body without checking;
      // the codec contract (javadoc above) is to return null on malformed input rather than
      // surface the parse exception to the caller. Caller treats null as "stat unavailable".
      LOG.debug("decode: numeric parse failed for tag '{}': {}", sanitizeTag(tag), e.getMessage());
      return null;
    }
  }

  private static Object decodeBody(String tag, String body) {
    // Length-cap the numeric / timestamp / date / boolean arms before parse. Float.parseFloat
    // and friends are O(n) in input length; without this guard a 10MB-digit body pins the
    // driver. Apply the cap only to short-fixed-width tags; the utf8/bin/dec arms have their
    // own larger caps above.
    switch (tag) {
      case "i32":
      case "i64":
      case "i16":
      case "i8":
      case "f32":
      case "f64":
      case "bool":
      case "date":
      case "ts":
      case "tsntz":
        if (body.length() > MAX_NUMERIC_BODY_LEN) {
          LOG.debug(
              "decode: tag '{}' body length {} exceeds MAX_NUMERIC_BODY_LEN={}",
              sanitizeTag(tag),
              body.length(),
              MAX_NUMERIC_BODY_LEN);
          return null;
        }
        break;
      default:
        // utf8/bin/dec/<unknown> use their own cap or the unrecognized-tag path below.
        break;
    }
    switch (tag) {
      case "i32":
        return Integer.parseInt(body);
      case "i64":
        return Long.parseLong(body);
      case "i16":
        return Short.parseShort(body);
      case "i8":
        return Byte.parseByte(body);
      case "f32":
        {
          float f = Float.parseFloat(body);
          // Symmetric to encode: NaN/Inf must never reach the CBO as a bound. A poisoned
          // TBLPROPERTIES entry like `f32:NaN` or `f32:Infinity` would otherwise mis-order
          // against finite values and cause empty-range pruning.
          if (Float.isNaN(f) || Float.isInfinite(f)) {
            return null;
          }
          return f;
        }
      case "f64":
        {
          double d = Double.parseDouble(body);
          if (Double.isNaN(d) || Double.isInfinite(d)) {
            return null;
          }
          return d;
        }
      case "bool":
        // Tighten over Boolean.parseBoolean's permissive semantics: only the exact strings
        // "true"/"false" (case-insensitive) round-trip the encoder. Anything else (e.g. a
        // planted "bool:yes" attempting to bias CBO selectivity) returns null per contract.
        if ("true".equalsIgnoreCase(body)) {
          return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(body)) {
          return Boolean.FALSE;
        }
        LOG.debug("decode: bool body '{}' is not 'true'/'false'", sanitizeTag(body));
        return null;
      case "utf8":
        if (body.length() > MAX_BASE64_BODY_LEN) {
          LOG.debug(
              "decode: utf8 body length {} exceeds MAX_BASE64_BODY_LEN={}",
              body.length(),
              MAX_BASE64_BODY_LEN);
          return null;
        }
        try {
          return new String(Base64.getDecoder().decode(body), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
          // Base64.getDecoder().decode throws on invalid-alphabet bytes; codec contract is
          // null on malformed. Catch here to honor the documented null-return.
          LOG.debug("decode: utf8 body is not valid base64");
          return null;
        }
      case "bin":
        if (body.length() > MAX_BASE64_BODY_LEN) {
          LOG.debug(
              "decode: bin body length {} exceeds MAX_BASE64_BODY_LEN={}",
              body.length(),
              MAX_BASE64_BODY_LEN);
          return null;
        }
        try {
          return Base64.getDecoder().decode(body);
        } catch (IllegalArgumentException e) {
          LOG.debug("decode: bin body is not valid base64");
          return null;
        }
      case "date":
        {
          // Days-since-epoch returned as Integer (Spark's CatalystTypeConverters.DateConverter
          // expects Int). Parse as long first so out-of-range values fail cleanly, then narrow.
          long days = Long.parseLong(body);
          if (days > Integer.MAX_VALUE || days < Integer.MIN_VALUE) {
            LOG.debug("decode: date days={} out of int range — treating as malformed", days);
            return null;
          }
          return (int) days;
        }
      case "ts":
        return Long.parseLong(body);
      case "tsntz":
        // Returned as Long microseconds-since-epoch (UTC). Spark's
        // CatalystTypeConverters.TimestampNTZConverter accepts Long internally for the catalyst
        // representation and converts to LocalDateTime when the optimizer needs the JVM type.
        return Long.parseLong(body);
      case "dec":
        {
          int scaleSep = body.indexOf(':');
          if (scaleSep < 0) {
            LOG.debug("decode: dec body missing scale separator ':'");
            return null;
          }
          // Bound scale length too: Integer.parseInt is O(n) but a giant scale string would
          // also signal a poisoned property. 12 chars covers MIN_VALUE/MAX_VALUE comfortably.
          if (scaleSep > 12) {
            LOG.debug("decode: dec scale length {} exceeds 12-char cap", scaleSep);
            return null;
          }
          String unscaledStr = body.substring(scaleSep + 1);
          // Guard against unbounded BigInteger(String) parse — O(n^2) in digit count.
          if (unscaledStr.length() > MAX_DECIMAL_BODY_LEN) {
            LOG.debug(
                "decode: dec unscaled body length {} exceeds MAX_DECIMAL_BODY_LEN={}",
                unscaledStr.length(),
                MAX_DECIMAL_BODY_LEN);
            return null;
          }
          int scale = Integer.parseInt(body.substring(0, scaleSep));
          java.math.BigInteger unscaled = new java.math.BigInteger(unscaledStr);
          return new BigDecimal(unscaled, scale);
        }
      default:
        LOG.debug("decode: unrecognized tag '{}'", sanitizeTag(tag));
        return null;
    }
  }

  /**
   * Strip CR/LF/tab from a tag value before passing to SLF4J. Tag is bounded to {@link
   * #MAX_TAG_LEN} characters at entry, so the only remaining log-injection vector is a tag value
   * containing control characters that the {} interpolation would render as a fake log line. This
   * mirrors the read-path's sanitizeForLog helper for TBLPROPERTIES values.
   */
  private static String sanitizeTag(String tag) {
    if (tag == null) {
      return null;
    }
    return tag.replace('\r', '_').replace('\n', '_').replace('\t', '_');
  }

  // ===== Histogram codec (v1) =====
  // Wire format (big-endian, base64-wrapped):
  //   [f64 height][u16 bin_count][per-bin: f64 lo][f64 hi][u64 ndv]
  //
  // height is persisted because Spark's CBO reads Histogram.height directly in selectivity
  // estimation; synthesizing 0.0 on decode would feed empty intervals to CBO. Non-finite
  // height/lo/hi rejected on both sides for the same reason.
  //
  // The decoder is not wired into the read path today — DSv2 ColumnStatistics exposes
  // histogram() but PersistedColumnStatistics returns empty pending future CBO integration.

  /**
   * Maximum number of bins this codec will encode or decode. Spark's default histogram cap is 254
   * bins; the headroom up to 1024 accommodates a future {@code spark.sql.statistics.
   * histogram.numBins} bump without requiring a wire-format change.
   */
  private static final int MAX_HISTOGRAM_BINS = 1024;

  /**
   * Hard cap on the base64-encoded histogram body. Realistic payload at 254 bins is ~8KB; 64KB is a
   * comfortable safety margin while bounding driver memory under a poisoned property.
   */
  private static final int MAX_HISTOGRAM_BODY_LEN = 65_536;

  private static final int HISTOGRAM_BIN_SIZE_BYTES = 8 + 8 + 8;
  private static final int HISTOGRAM_HEADER_BYTES = 8 + 2; // height (f64) + bin_count (u16)

  /**
   * Encode a Spark catalyst {@link Histogram} into a base64 wire form under {@link
   * LanceStatsKeys#HISTOGRAM_FORMAT_V1}. Returns {@code null} when the input is null, has no bins,
   * exceeds {@link #MAX_HISTOGRAM_BINS}, has a NaN/±Inf height or bin bound, or has a negative
   * NDV — caller writes nothing for that column rather than persist a poisonable stat.
   */
  public static String encodeHistogram(Histogram h) {
    if (h == null) {
      LOG.debug("encodeHistogram: null histogram");
      return null;
    }
    double height = h.height();
    if (Double.isNaN(height) || Double.isInfinite(height)) {
      LOG.debug("encodeHistogram: non-finite height={}", height);
      return null;
    }
    HistogramBin[] bins = h.bins();
    if (bins == null || bins.length == 0) {
      LOG.debug("encodeHistogram: empty bins");
      return null;
    }
    if (bins.length > MAX_HISTOGRAM_BINS) {
      LOG.debug(
          "encodeHistogram: bin count {} exceeds MAX_HISTOGRAM_BINS={}",
          bins.length,
          MAX_HISTOGRAM_BINS);
      return null;
    }
    ByteBuffer buf =
        ByteBuffer.allocate(HISTOGRAM_HEADER_BYTES + bins.length * HISTOGRAM_BIN_SIZE_BYTES)
            .order(ByteOrder.BIG_ENDIAN);
    buf.putDouble(height);
    buf.putShort((short) bins.length);
    for (HistogramBin bin : bins) {
      if (bin == null) {
        LOG.debug("encodeHistogram: null bin at offset");
        return null;
      }
      double lo = bin.lo();
      double hi = bin.hi();
      if (Double.isNaN(lo) || Double.isInfinite(lo) || Double.isNaN(hi) || Double.isInfinite(hi)) {
        LOG.debug("encodeHistogram: non-finite bin bound (lo={}, hi={})", lo, hi);
        return null;
      }
      long ndv = bin.ndv();
      if (ndv < 0L) {
        LOG.debug("encodeHistogram: negative bin ndv={}", ndv);
        return null;
      }
      buf.putDouble(lo);
      buf.putDouble(hi);
      buf.putLong(ndv);
    }
    return Base64.getEncoder().encodeToString(buf.array());
  }

  /**
   * Decode a base64 histogram body produced by {@link #encodeHistogram}. Returns {@code null} when
   * the format tag is not {@link LanceStatsKeys#HISTOGRAM_FORMAT_V1}, the body exceeds {@link
   * #MAX_HISTOGRAM_BODY_LEN}, any bound is NaN/±Inf, any NDV is negative, the bin count exceeds
   * {@link #MAX_HISTOGRAM_BINS}, or the payload is truncated / malformed. Never throws.
   */
  public static Histogram decodeHistogram(String encoded, String formatTag) {
    if (encoded == null || formatTag == null) {
      LOG.debug(
          "decodeHistogram: null input (encoded null={}, format null={})",
          encoded == null,
          formatTag == null);
      return null;
    }
    if (!LanceStatsKeys.HISTOGRAM_FORMAT_V1.equals(formatTag.trim())) {
      LOG.debug("decodeHistogram: unrecognized format tag '{}'", sanitizeTag(formatTag));
      return null;
    }
    if (encoded.length() > MAX_HISTOGRAM_BODY_LEN) {
      LOG.debug(
          "decodeHistogram: body length {} exceeds MAX_HISTOGRAM_BODY_LEN={}",
          encoded.length(),
          MAX_HISTOGRAM_BODY_LEN);
      return null;
    }
    byte[] raw;
    try {
      raw = Base64.getDecoder().decode(encoded);
    } catch (IllegalArgumentException e) {
      LOG.debug("decodeHistogram: invalid base64");
      return null;
    }
    if (raw.length < HISTOGRAM_HEADER_BYTES) {
      LOG.debug("decodeHistogram: body too short for header (length={})", raw.length);
      return null;
    }
    ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
    double height = buf.getDouble();
    if (Double.isNaN(height) || Double.isInfinite(height)) {
      LOG.debug("decodeHistogram: non-finite height={}", height);
      return null;
    }
    int binCount = buf.getShort() & 0xFFFF;
    if (binCount == 0 || binCount > MAX_HISTOGRAM_BINS) {
      LOG.debug("decodeHistogram: bin count {} out of range [1, {}]", binCount, MAX_HISTOGRAM_BINS);
      return null;
    }
    int expectedLen = HISTOGRAM_HEADER_BYTES + binCount * HISTOGRAM_BIN_SIZE_BYTES;
    if (raw.length != expectedLen) {
      LOG.debug(
          "decodeHistogram: body length {} != expected {} for bin count {}",
          raw.length,
          expectedLen,
          binCount);
      return null;
    }
    HistogramBin[] bins = new HistogramBin[binCount];
    for (int i = 0; i < binCount; i++) {
      double lo = buf.getDouble();
      double hi = buf.getDouble();
      long ndv = buf.getLong();
      if (Double.isNaN(lo) || Double.isInfinite(lo) || Double.isNaN(hi) || Double.isInfinite(hi)) {
        LOG.debug("decodeHistogram: non-finite bin bound at bin {} (lo={}, hi={})", i, lo, hi);
        return null;
      }
      if (ndv < 0L) {
        LOG.debug("decodeHistogram: negative bin ndv at bin {} ({})", i, ndv);
        return null;
      }
      bins[i] = new HistogramBin(lo, hi, ndv);
    }
    return new Histogram(height, bins);
  }
}
