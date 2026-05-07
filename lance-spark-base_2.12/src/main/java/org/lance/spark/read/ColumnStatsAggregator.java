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

import org.lance.index.scalar.ZoneStats;

import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.connector.read.colstats.Histogram;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Aggregates per-zone {@link ZoneStats} loaded from a Lance zonemap index into a Spark DSv2 {@link
 * ColumnStatistics} value (min/max/nullCount). Pure function — no I/O, no Spark session access — so
 * it is straightforward to unit-test.
 *
 * <p>Reduction rules:
 *
 * <ul>
 *   <li>{@code min} = the smallest non-null zone min across all zones.
 *   <li>{@code max} = the largest non-null zone max across all zones.
 *   <li>{@code nullCount} = sum of zone null counts.
 *   <li>If every zone is all-null (both {@code min} and {@code max} are {@code null}), only the
 *       null count is reported and min/max are absent.
 *   <li>If zones disagree on the runtime class of {@code min}/{@code max} (e.g. some {@code Long},
 *       some {@code Integer}), the column is skipped — comparing across types via {@link
 *       Comparable#compareTo} would throw. Callers see {@link Optional#empty()}.
 * </ul>
 *
 * <p>Phase 1 of the CBO enablement plan reports {@code min}/{@code max}/{@code nullCount}. Phase 3a
 * adds a <b>conservative NDV</b> estimate when every zone has {@code min == max} (a single distinct
 * value per zone): {@code distinctCount} = the count of distinct zone-min values. If any zone has
 * {@code min != max} we cannot bound the column's distinct values from zone metadata alone, so
 * {@code distinctCount} is left empty and Spark's CBO falls back to row-count heuristics. The
 * remaining {@link ColumnStatistics} fields ({@link ColumnStatistics#avgLen}, {@link
 * ColumnStatistics#maxLen}, {@link ColumnStatistics#histogram}) still fall back to the interface
 * default ({@link OptionalLong#empty()} / {@link Optional#empty()}) because Lance zonemap stats do
 * not carry that data today.
 */
public final class ColumnStatsAggregator {

  private ColumnStatsAggregator() {}

  /**
   * Aggregate a column's per-zone stats into a single {@link ColumnStatistics}.
   *
   * @param zones the per-zone stats from {@code Dataset.getZonemapStats(column)}; may be empty
   * @return aggregated stats, or {@link Optional#empty()} when there is nothing to report or the
   *     zone runtime types disagree
   */
  public static Optional<ColumnStatistics> aggregate(List<ZoneStats> zones) {
    if (zones == null || zones.isEmpty()) {
      return Optional.empty();
    }

    Comparable<?> globalMin = null;
    Comparable<?> globalMax = null;
    long totalNulls = 0L;
    Class<?> seenClass = null;
    boolean sawAnyValue = false;

    for (ZoneStats zone : zones) {
      totalNulls += zone.getNullCount();
      Comparable<?> zMin = zone.getMin();
      Comparable<?> zMax = zone.getMax();
      // Coerce non-finite bounds (NaN / ±Inf) to null. CBO uses min/max as hard predicate
      // bounds, and a non-finite min compares "greater than" every finite value, producing
      // empty ranges that prune rows satisfying the predicate. Symmetric with the persisted-
      // path rejection in ColumnStatsCodec.
      if (isNonFinite(zMin)) {
        zMin = null;
      }
      if (isNonFinite(zMax)) {
        zMax = null;
      }
      if (zMin == null && zMax == null) {
        continue;
      }
      sawAnyValue = true;

      // Verify both zMin and zMax against `seenClass` independently. Checking only one (or
      // checking against a `probe` derived from "the first non-null bound") would silently
      // accept a malformed zone where zMin and zMax have different runtime classes — e.g.
      // zone with Long min and String max — and the later compareTo() call would throw at
      // query time instead of being rejected up front here.
      if (zMin != null) {
        if (seenClass == null) {
          seenClass = zMin.getClass();
        } else if (!seenClass.equals(zMin.getClass())) {
          return Optional.empty();
        }
      }
      if (zMax != null) {
        if (seenClass == null) {
          seenClass = zMax.getClass();
        } else if (!seenClass.equals(zMax.getClass())) {
          return Optional.empty();
        }
      }

      if (zMin != null && (globalMin == null || compare(zMin, globalMin) < 0)) {
        globalMin = zMin;
      }
      if (zMax != null && (globalMax == null || compare(zMax, globalMax) > 0)) {
        globalMax = zMax;
      }
    }

    if (!sawAnyValue && totalNulls == 0L) {
      return Optional.empty();
    }

    Long conservativeNdv = computeConservativeNdv(zones);
    return Optional.of(
        new ZoneStatsBackedColumnStatistics(globalMin, globalMax, totalNulls, conservativeNdv));
  }

  /**
   * Compute a conservative NDV estimate when every non-null zone has {@code min == max}. Each such
   * zone contributes exactly one distinct value; the column's NDV is bounded above by the number of
   * distinct zone-min values (could be lower if the same value appears in multiple zones, hence
   * conservative). Returns {@code null} when any non-all-null zone fails the single-value test —
   * either {@code zMin != zMax} (a multi-value zone) or {@code zMin}/{@code zMax} are partially
   * {@code null} (a "half-null" zone with one bound unknown), since neither case lets zone-level
   * metadata bound distinct count.
   *
   * <p>This is the Phase 3a "conservative NDV" path — most useful for low-cardinality columns (e.g.
   * {@code d_year}, {@code ca_state}, {@code cd_marital_status}) where the per-zone "single
   * distinct value" pattern holds naturally for sorted / clustered columns.
   */
  private static Long computeConservativeNdv(List<ZoneStats> zones) {
    Set<Object> distinctZoneValues = new HashSet<>();
    for (ZoneStats zone : zones) {
      Comparable<?> zMin = zone.getMin();
      Comparable<?> zMax = zone.getMax();
      // Coerce non-finite bounds (NaN / ±Inf) to null — see aggregate(). A non-finite as a
      // "single-distinct-value" would mislead the NDV count; better to treat the zone as
      // partially-bounded and bail out.
      if (isNonFinite(zMin)) {
        zMin = null;
      }
      if (isNonFinite(zMax)) {
        zMax = null;
      }
      if (zMin == null && zMax == null) {
        continue; // all-null zone contributes nothing
      }
      if (zMin == null || zMax == null || !zMin.equals(zMax)) {
        // Zone covers multiple distinct values — we can't conclude NDV from zone metadata.
        return null;
      }
      distinctZoneValues.add(zMin);
    }
    return distinctZoneValues.isEmpty() ? null : (long) distinctZoneValues.size();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static int compare(Comparable a, Comparable b) {
    return a.compareTo(b);
  }

  /**
   * True for non-finite Float/Double values (NaN, ±Infinity). Treated as "no bound on this side"
   * because a non-finite min/max mis-orders against finite values and would make CBO compute empty
   * ranges. Mirrors {@link ColumnStatsCodec}'s encode/decode rejection.
   */
  private static boolean isNonFinite(Object value) {
    if (value instanceof Float) {
      Float f = (Float) value;
      return f.isNaN() || f.isInfinite();
    }
    if (value instanceof Double) {
      Double d = (Double) value;
      return d.isNaN() || d.isInfinite();
    }
    return false;
  }

  /**
   * Concrete {@link ColumnStatistics} backed by zone-aggregated min/max/nullCount. Returns the
   * stored {@link Comparable} values directly — Spark's V2 → Catalyst bridge ({@code
   * V2ColumnStats}) calls {@code CatalystTypeConverters.convertToCatalyst} on each value with the
   * column's {@link org.apache.spark.sql.types.DataType} when the optimizer reads it, so passing
   * Java-native types (Integer, Long, Double, String) is safe.
   */
  static final class ZoneStatsBackedColumnStatistics
      implements ColumnStatistics, java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final Comparable<?> min;
    private final Comparable<?> max;
    private final long nullCount;

    /** Conservative NDV from zone-min counting, or {@code null} when not derivable. */
    private final Long distinctCount;

    ZoneStatsBackedColumnStatistics(Comparable<?> min, Comparable<?> max, long nullCount) {
      this(min, max, nullCount, null);
    }

    ZoneStatsBackedColumnStatistics(
        Comparable<?> min, Comparable<?> max, long nullCount, Long distinctCount) {
      this.min = min;
      this.max = max;
      this.nullCount = nullCount;
      this.distinctCount = distinctCount;
    }

    @Override
    public OptionalLong distinctCount() {
      return distinctCount == null ? OptionalLong.empty() : OptionalLong.of(distinctCount);
    }

    @Override
    public Optional<Object> min() {
      return min == null ? Optional.empty() : Optional.of(min);
    }

    @Override
    public Optional<Object> max() {
      return max == null ? Optional.empty() : Optional.of(max);
    }

    @Override
    public OptionalLong nullCount() {
      return OptionalLong.of(nullCount);
    }

    @Override
    public OptionalLong avgLen() {
      return OptionalLong.empty();
    }

    @Override
    public OptionalLong maxLen() {
      return OptionalLong.empty();
    }

    @Override
    public Optional<Histogram> histogram() {
      return Optional.empty();
    }
  }
}
