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

import org.lance.Dataset;
import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.ColumnOrdering;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.LanceDatasetCache;
import org.lance.spark.internal.LanceExceptions;
import org.lance.spark.internal.LanceFragmentScanner;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.arrow.util.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.internal.connector.SupportsMetadata;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LanceScan
    implements Batch,
        Scan,
        SupportsMetadata,
        SupportsReportStatistics,
        SupportsReportPartitioning,
        Serializable {
  private static final long serialVersionUID = 947284762748623947L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceScan.class);

  private final StructType schema;
  private final LanceSparkReadOptions readOptions;
  private final Optional<String> whereConditions;
  private final Optional<Integer> limit;
  private final Optional<Integer> offset;
  private final Optional<List<ColumnOrdering>> topNSortOrders;
  private final Optional<Aggregation> pushedAggregation;
  private final Filter[] pushedFilters;
  private final LanceStatistics statistics;
  private final String scanId = UUID.randomUUID().toString();

  /**
   * Per-column zonemap statistics loaded on the driver during scan building. Used for
   * fragment-level pruning in {@link #planInputPartitions()}.
   */
  private final java.util.Map<String, List<ZoneStats>> zonemapStats;

  /**
   * Pre-computed surviving fragment IDs from zonemap pruning in LanceScanBuilder. When non-null,
   * {@link #pruneByZonemapStats} skips re-computing and uses these directly.
   */
  private final Set<Integer> cachedSurvivingFragmentIds;

  /** Number of partitions after pruning, set during {@link #planInputPartitions()}. */
  private transient int numPartitions = -1;

  /**
   * Partition info detected from zonemap stats. When present, enables storage-partitioned joins
   * (SPJ) by reporting the partition column as the output partitioning key instead of {@code
   * _fragid}. Null when no partition-compatible column is detected.
   */
  private final ZonemapFragmentPruner.PartitionInfo partitionInfo;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  /**
   * Pre-computed scan plan (splits + resolved version + per-fragment row counts) harvested by
   * {@link LanceScanBuilder#build()} from the pushdown-phase Dataset handle. Marked {@code
   * transient} because it is driver-only: {@link #planInputPartitions()} is invoked on the driver,
   * so after Spark ships the Scan to executors this field is null — but executors never call {@code
   * planInputPartitions()}. A null value on the driver (e.g. a future caller constructing LanceScan
   * directly) triggers the legacy open-and-plan fallback.
   */
  private final transient LanceSplit.ScanPlanResult preplannedResult;

  public LanceScan(
      StructType schema,
      LanceSparkReadOptions readOptions,
      Optional<String> whereConditions,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders,
      Optional<Aggregation> pushedAggregation,
      Filter[] pushedFilters,
      LanceStatistics statistics,
      java.util.Map<String, List<ZoneStats>> zonemapStats,
      Set<Integer> survivingFragmentIds,
      ZonemapFragmentPruner.PartitionInfo partitionInfo,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties,
      LanceSplit.ScanPlanResult preplannedResult) {
    this.schema = schema;
    this.readOptions = readOptions;
    this.whereConditions = whereConditions;
    this.limit = limit;
    this.offset = offset;
    this.topNSortOrders = topNSortOrders;
    this.pushedAggregation = pushedAggregation;
    this.pushedFilters =
        pushedFilters != null ? Arrays.copyOf(pushedFilters, pushedFilters.length) : new Filter[0];
    this.statistics = statistics;
    this.zonemapStats = zonemapStats != null ? zonemapStats : Collections.emptyMap();
    this.cachedSurvivingFragmentIds = survivingFragmentIds;
    this.partitionInfo = partitionInfo;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.preplannedResult = preplannedResult;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    LanceSplit.ScanPlanResult planResult =
        (preplannedResult != null) ? preplannedResult : planFallback();
    List<LanceSplit> prunedSplits = pruneByRowAddrFilters(planResult.getSplits());

    // Zonemap-based fragment pruning: uses per-column min/max/null_count
    // statistics to eliminate fragments that provably cannot match
    // pushed filters.
    prunedSplits = pruneByZonemapStats(prunedSplits);

    // Limit-based split pruning: when a LIMIT is pushed down without filters or TopN sort,
    // use per-fragment row counts to plan only enough splits to satisfy the limit.
    // This avoids scheduling hundreds of unnecessary tasks. Correctness is guaranteed
    // because Spark still keeps a global CollectLimit on top (isPartiallyPushed = true).
    prunedSplits = pruneByLimit(prunedSplits, planResult.getFragmentRowCounts());

    // Capture as effectively final for use in lambda
    final List<LanceSplit> finalSplits = prunedSplits;

    // Use resolved version for snapshot isolation - ensures all workers read the same version.
    // Math.toIntExact fails loudly on overflow; consistent with the narrowing in
    // LanceScanBuilder.getOrOpenDataset() so both planning paths agree on range behavior.
    //
    // Fast-path: when the driver already pinned the exact same version (Option B path via
    // LanceScanBuilder.getOrOpenDataset) we skip `withVersion` to keep the original
    // LanceSparkReadOptions *object identity*. That guarantees the executor-side
    // LanceFragmentScanner.buildCacheKey(...) produces a cache key that is byte-identical to
    // the driver's insert — any map-copy performed by withVersion could subtly perturb
    // iteration order / identity of the storageOptions map and miss the warm cache entry.
    long resolvedVersion = planResult.getResolvedVersion();
    Integer currentPinned = readOptions.getVersion();
    LanceSparkReadOptions resolvedReadOptions =
        (currentPinned != null && currentPinned.longValue() == resolvedVersion)
            ? readOptions
            : readOptions.withVersion(Math.toIntExact(resolvedVersion));

    // Pre-compute preferred cache locations for each split.
    String[][] cacheLocations = new String[finalSplits.size()][];
    if (org.lance.spark.internal.LanceExecutorCache.isEnabled()) {
      org.lance.spark.internal.LanceCacheLocationTracker tracker =
          org.lance.spark.internal.LanceCacheLocationTracker.getInstance();
      java.util.Map<String, String> baseOpts =
          resolvedReadOptions.getStorageOptions() != null
              ? resolvedReadOptions.getStorageOptions()
              : java.util.Collections.emptyMap();
      java.util.Map<String, String> mergedOpts =
          org.lance.spark.LanceRuntime.mergeStorageOptions(
              baseOpts,
              initialStorageOptions != null
                  ? initialStorageOptions
                  : java.util.Collections.emptyMap());
      for (int idx = 0; idx < finalSplits.size(); idx++) {
        try {
          int fragId = finalSplits.get(idx).getFragments().get(0);
          org.lance.spark.internal.LanceExecutorCacheKey key =
              new org.lance.spark.internal.LanceExecutorCacheKey(
                  resolvedReadOptions.getDatasetUri(),
                  resolvedReadOptions.getVersion(),
                  fragId,
                  resolvedReadOptions.getBatchSize(),
                  mergedOpts);
          cacheLocations[idx] = tracker.getPreferredLocations(key.fingerprint());
        } catch (Exception e) {
          cacheLocations[idx] = new String[0];
        }
      }
    } else {
      java.util.Arrays.fill(cacheLocations, new String[0]);
    }

    InputPartition[] result =
        IntStream.range(0, finalSplits.size())
            .mapToObj(
                i -> {
                  LanceSplit split = finalSplits.get(i);
                  InternalRow partKeyRow = null;
                  if (partitionInfo != null) {
                    int fragId = split.getFragments().get(0);
                    partKeyRow = partitionInfo.partitionKeyForFragment(fragId);
                  }
                  return new LanceInputPartition(
                      schema,
                      i,
                      split,
                      resolvedReadOptions,
                      whereConditions,
                      limit,
                      offset,
                      topNSortOrders,
                      pushedAggregation,
                      scanId,
                      initialStorageOptions,
                      namespaceImpl,
                      namespaceProperties,
                      partKeyRow,
                      cacheLocations[i]);
                })
            .toArray(InputPartition[]::new);

    this.numPartitions = result.length;
    return result;
  }

  /**
   * Legacy/direct-construction fallback used when {@link LanceScanBuilder#build()} did not supply a
   * {@link LanceSplit.ScanPlanResult} via the constructor (e.g. tests that instantiate {@link
   * LanceScan} directly, or future callers outside the standard pushdown pipeline).
   *
   * <p>The version is resolved cheaply ahead of planScan so the fallback is symmetric with {@link
   * LanceScanBuilder#getOrOpenDataset()} — both paths pin a specific version before opening the
   * Dataset. That keeps the "driver plans, executors hit cache by pinned version" invariant intact
   * even when an external driver pass has already seeded {@link
   * org.lance.spark.internal.LanceDatasetCache} with a specific version.
   */
  private LanceSplit.ScanPlanResult planFallback() {
    LanceSparkReadOptions pinned = readOptions;
    if (pinned.getVersion() == null) {
      java.util.Map<String, String> base =
          readOptions.getStorageOptions() != null
              ? readOptions.getStorageOptions()
              : Collections.emptyMap();
      java.util.Map<String, String> merged =
          LanceRuntime.mergeStorageOptions(base, initialStorageOptions);
      long resolved;
      try {
        resolved = Dataset.latestVersionId(readOptions.getDatasetUri(), merged);
      } catch (RuntimeException ex) {
        // Upstream Lance (JNI) errors embed the raw URI — including userinfo / SAS tokens /
        // signed-URL query params — into the exception message. Preserving `ex` as cause would
        // leak those credentials through `Throwable.printStackTrace`, Spark UI's "Full stacktrace",
        // and SLF4J `{}`-with-throwable formatting. Drop the cause; only retain the underlying
        // exception's class name for diagnosis.
        throw LanceExceptions.wrap("resolve latest Lance dataset version", ex);
      }
      try {
        pinned = readOptions.withVersion(Math.toIntExact(resolved));
      } catch (ArithmeticException ex) {
        // Do not include the URI in the exception message. ArithmeticException from
        // Math.toIntExact does NOT embed the URI, so preserving it as cause is safe.
        throw new IllegalStateException(
            "Lance dataset version "
                + resolved
                + " exceeds int range; cannot pin for snapshot isolation",
            ex);
      }
    }
    // When dataset_cache_enabled=true, route the fallback open through LanceDatasetCache so the
    // entry is warm for subsequent executor checkouts on the same JVM (same invariant as
    // LanceScanBuilder.getOrOpenDataset). Outside the cache path, just forward
    // initialStorageOptions so vended credentials from namespace.describeTable() flow through —
    // namespace-managed tables (HMS, REST catalog, LanceDB cloud) need the STS token the base
    // storage options don't carry.
    if (pinned.isDatasetCacheEnabled()) {
      LanceDatasetCache.CacheKey key =
          LanceFragmentScanner.buildCacheKey(pinned, initialStorageOptions);
      final LanceSparkReadOptions opts = pinned;
      final java.util.Map<String, String> initial = initialStorageOptions;
      // Wrap both the checkout() open AND the subsequent planScan() in a URI-redacting try/catch.
      // Upstream object-store errors (403/404/429 on manifest HEAD, parse errors, region
      // mismatches)
      // embed the raw URI — including userinfo / SAS tokens / signed-URL query params — into the
      // exception message. Preserving `ex` as cause would leak those credentials through
      // Throwable.printStackTrace, Spark UI "Full stacktrace", and SLF4J `{}`-with-throwable
      // formatting. Drop the cause; only retain the underlying exception's class name.
      Dataset dataset;
      try {
        dataset =
            LanceDatasetCache.checkout(
                key, () -> Utils.openDatasetBuilder(opts).initialStorageOptions(initial).build());
      } catch (RuntimeException ex) {
        throw LanceExceptions.wrap("open Lance dataset", ex);
      }
      try {
        return LanceSplit.planScan(dataset);
      } catch (RuntimeException ex) {
        throw LanceExceptions.wrap("plan Lance scan", ex);
      } finally {
        LanceDatasetCache.release(key);
      }
    }
    try {
      return LanceSplit.planScan(pinned, initialStorageOptions);
    } catch (RuntimeException ex) {
      throw LanceExceptions.wrap("plan Lance scan", ex);
    }
  }

  /**
   * Prunes splits based on {@code _rowaddr} filters — skipping fragment opens, scan setup, and task
   * scheduling for fragments that provably cannot match the query predicate.
   *
   * <p>CONTRACT: {@link LanceSplit#getFragments()} returns Lance fragment IDs as Integer values
   * that match {@code (int)(rowAddr >>> 32)} — the same encoding used by {@link
   * org.lance.spark.join.FragmentAwareJoinUtils}. This is verified by {@link
   * LanceSplit#planScan(LanceSparkReadOptions)} which maps {@code Fragment.getId()} directly.
   *
   * <p>Note: an empty allowedIds set is valid — it means the filter is unsatisfiable (e.g. {@code

  /**
   * _rowaddr = 0 AND _rowaddr = 4294967296L}) and no fragments can match, resulting in zero rows
   * returned.
   */
  private List<LanceSplit> pruneByRowAddrFilters(List<LanceSplit> allSplits) {
    java.util.Optional<Set<Integer>> targetFragmentIds =
        RowAddressFilterAnalyzer.extractTargetFragmentIds(pushedFilters);
    if (!targetFragmentIds.isPresent()) {
      return allSplits;
    }
    Set<Integer> allowedIds = targetFragmentIds.get();
    // Assumes each LanceSplit maps to a single fragment. If splits ever
    // bundle multiple fragments, consider sub-split level pruning.
    List<LanceSplit> pruned =
        allSplits.stream()
            .filter(
                split -> {
                  if (split.getFragments().size() > 1) {
                    LOG.warn(
                        "Split contains {} fragments;" + " sub-split pruning not implemented",
                        split.getFragments().size());
                  }
                  return split.getFragments().stream().anyMatch(allowedIds::contains);
                })
            .collect(Collectors.toList());
    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Pruned fragments by _rowaddr filters: {} of {} splits retained,"
              + " allowed fragment IDs: {}",
          pruned.size(),
          allSplits.size(),
          allowedIds);
    } else {
      LOG.debug(
          "No fragments pruned by _rowaddr filters: all {} splits retained,"
              + " allowed fragment IDs: {}",
          allSplits.size(),
          allowedIds);
    }
    return pruned;
  }

  /**
   * Prunes splits based on pushed LIMIT using per-fragment row counts from the manifest.
   *
   * <p>When a LIMIT is pushed down without filters or TopN sort orders, we can use the per-fragment
   * logical row counts (which account for deletions) to determine how many fragments are needed to
   * satisfy the limit. This avoids scheduling hundreds of unnecessary tasks for large tables.
   *
   * <p>This optimization is skipped when:
   *
   * <ul>
   *   <li>No limit is pushed
   *   <li>Filters are present (unknown selectivity makes row count estimation unreliable)
   *   <li>TopN sort orders are present (all fragments needed for global sort)
   *   <li>Aggregation is pushed (e.g., COUNT(*) LIMIT — row counts don't apply)
   *   <li>Vector search (nearest) is active (needs global search across all fragments)
   *   <li>Fragment row counts are unavailable
   * </ul>
   *
   * <p>Correctness is guaranteed because Spark keeps a global {@code CollectLimit} on top (since
   * {@code isPartiallyPushed()} returns {@code true}). If we under-estimate due to concurrent
   * deletions, the query simply returns fewer rows than the limit — which is valid LIMIT semantics.
   */
  private List<LanceSplit> pruneByLimit(
      List<LanceSplit> allSplits, java.util.Map<Integer, Long> fragmentRowCounts) {
    if (!limit.isPresent()
        || whereConditions.isPresent()
        || topNSortOrders.isPresent()
        || pushedAggregation.isPresent()
        || readOptions.getNearest() != null
        || fragmentRowCounts.isEmpty()) {
      return allSplits;
    }

    int requestedLimit = limit.get();
    long rowsAccumulated = 0;
    List<LanceSplit> pruned = new java.util.ArrayList<>();

    for (LanceSplit split : allSplits) {
      pruned.add(split);
      for (int fragmentId : split.getFragments()) {
        Long rowCount = fragmentRowCounts.get(fragmentId);
        if (rowCount != null) {
          rowsAccumulated += rowCount;
        }
      }
      if (rowsAccumulated >= requestedLimit) {
        break;
      }
    }

    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Limit-based pruning: {} of {} splits retained for LIMIT {} "
              + "(accumulated {} rows from selected fragments)",
          pruned.size(),
          allSplits.size(),
          requestedLimit,
          rowsAccumulated);
    }

    return pruned;
  }

  /**
   * Prunes splits based on zonemap index statistics — using per-column min/max/null_count to
   * eliminate fragments that provably cannot match the pushed filters.
   *
   * <p>This is analogous to partition pruning in Hive/Iceberg: fragments whose zones all fail the
   * predicate are skipped entirely, avoiding fragment opens, scan setup, and task scheduling.
   */
  private List<LanceSplit> pruneByZonemapStats(List<LanceSplit> allSplits) {
    // Use cached result from LanceScanBuilder if available, otherwise compute.
    Set<Integer> allowedIds;
    if (cachedSurvivingFragmentIds != null) {
      allowedIds = cachedSurvivingFragmentIds;
    } else if (!zonemapStats.isEmpty()) {
      allowedIds = ZonemapFragmentPruner.pruneFragments(pushedFilters, zonemapStats).orElse(null);
    } else {
      return allSplits;
    }

    if (allowedIds == null) {
      return allSplits;
    }
    List<LanceSplit> pruned =
        allSplits.stream()
            .filter(split -> split.getFragments().stream().anyMatch(allowedIds::contains))
            .collect(Collectors.toList());

    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Zonemap pruning: {} of {} splits retained," + " allowed fragment IDs: {}",
          pruned.size(),
          allSplits.size(),
          allowedIds);
    }

    return pruned;
  }

  /**
   * Reports the output partitioning to Spark's optimizer.
   *
   * <p>When a partition-compatible column is detected via zonemap stats (every fragment has a
   * single distinct value for that column), we report the data column as the partition key. This
   * enables Spark's storage-partitioned join (SPJ) protocol — allowing shuffle-free joins between
   * Lance tables or between Lance and other data sources (e.g., Iceberg) that share the same
   * partition column.
   *
   * <p>When no partition column is detected, returns {@link UnknownPartitioning}.
   */
  @Override
  public Partitioning outputPartitioning() {
    if (partitionInfo != null) {
      // Use partition info fragment count — available before
      // planInputPartitions() is called. This allows
      // V2ScanPartitioningAndOrdering to see the partitioning
      // early enough for SPJ.
      int partCount =
          numPartitions >= 0 ? numPartitions : partitionInfo.getFragmentPartitionValues().size();
      Expression[] keys = new Expression[] {FieldReference.apply(partitionInfo.getColumnName())};
      return new KeyGroupedPartitioning(keys, partCount);
    }
    return new UnknownPartitioning(numPartitions >= 0 ? numPartitions : 0);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceReaderFactory();
  }

  @Override
  public StructType readSchema() {
    if (pushedAggregation.isPresent()) {
      return new StructType().add("count", org.apache.spark.sql.types.DataTypes.LongType);
    }
    return schema;
  }

  @Override
  public Map<String, String> getMetaData() {
    scala.collection.immutable.Map<String, String> empty =
        scala.collection.immutable.Map$.MODULE$.empty();
    scala.collection.immutable.Map<String, String> result = empty;
    result = result.$plus(scala.Tuple2.apply("whereConditions", whereConditions.toString()));
    result = result.$plus(scala.Tuple2.apply("limit", limit.toString()));
    result = result.$plus(scala.Tuple2.apply("offset", offset.toString()));
    result = result.$plus(scala.Tuple2.apply("topNSortOrders", topNSortOrders.toString()));
    result = result.$plus(scala.Tuple2.apply("pushedAggregation", pushedAggregation.toString()));
    return result;
  }

  @Override
  public Statistics estimateStatistics() {
    return statistics;
  }

  /**
   * Required for Spark's ReusedExchange: {@code BatchScanExec.equals()} compares {@code batch}
   * objects, which delegate to this method since LanceScan implements Batch.
   *
   * <p>Excludes {@code scanId} (per-instance tracing UUID, not scan identity).
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceScan that = (LanceScan) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(readOptions, that.readOptions)
        && Objects.equals(whereConditions, that.whereConditions)
        && Objects.equals(limit, that.limit)
        && Objects.equals(offset, that.offset)
        && Objects.equals(topNSortOrders.toString(), that.topNSortOrders.toString())
        && aggregationEquals(pushedAggregation, that.pushedAggregation)
        && equivalentFilters(pushedFilters, that.pushedFilters);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            schema, readOptions, whereConditions, limit, offset, topNSortOrders.toString());
    result = 31 * result + Arrays.hashCode(sortedByHash(pushedFilters));
    result = 31 * result + aggregationHashCode(pushedAggregation);
    return result;
  }

  /**
   * Compares two Optional&lt;Aggregation&gt; by value. {@code Aggregation}'s auto-generated {@code
   * equals()} uses reference identity for its array components, so we sort by hashCode and compare
   * element-wise — following {@code AggregatePushDownUtils.equivalentAggregations()}.
   */
  private static boolean aggregationEquals(Optional<Aggregation> a, Optional<Aggregation> b) {
    if (a.isPresent() != b.isPresent()) {
      return false;
    }
    if (!a.isPresent()) {
      return true;
    }
    Aggregation agg1 = a.get();
    Aggregation agg2 = b.get();
    return Arrays.equals(
            sortedByHash(agg1.aggregateExpressions()), sortedByHash(agg2.aggregateExpressions()))
        && Arrays.equals(
            sortedByHash(agg1.groupByExpressions()), sortedByHash(agg2.groupByExpressions()));
  }

  private static int aggregationHashCode(Optional<Aggregation> agg) {
    if (!agg.isPresent()) {
      return 0;
    }
    return Objects.hash(
        Arrays.hashCode(sortedByHash(agg.get().aggregateExpressions())),
        Arrays.hashCode(sortedByHash(agg.get().groupByExpressions())));
  }

  /**
   * Returns whether two filter arrays are equivalent regardless of order. Follows Spark's {@code
   * FileScan.equivalentFilters()}: sort by hashCode, then compare element-wise.
   */
  private static boolean equivalentFilters(Filter[] a, Filter[] b) {
    return Arrays.equals(sortedByHash(a), sortedByHash(b));
  }

  private static <T> T[] sortedByHash(T[] arr) {
    T[] copy = Arrays.copyOf(arr, arr.length);
    Arrays.sort(copy, (x, y) -> Integer.compare(x.hashCode(), y.hashCode()));
    return copy;
  }

  private static class LanceReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      Preconditions.checkArgument(
          partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");
      return LanceRowPartitionReader.create((LanceInputPartition) partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      Preconditions.checkArgument(
          partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");

      LanceInputPartition lancePartition = (LanceInputPartition) partition;
      if (lancePartition.getPushedAggregation().isPresent()) {
        AggregateFunc[] aggFunc =
            lancePartition.getPushedAggregation().get().aggregateExpressions();
        if (aggFunc.length == 1 && aggFunc[0] instanceof CountStar) {
          return new LanceCountStarPartitionReader(lancePartition);
        }
      }

      return new LanceColumnarPartitionReader(lancePartition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return true;
    }
  }
}
