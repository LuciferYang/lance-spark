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
import org.lance.Fragment;
import org.lance.ManifestSummary;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.ColumnOrdering;
import org.lance.schema.LanceField;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.internal.LanceDatasetCache;
import org.lance.spark.internal.LanceExceptions;
import org.lance.spark.internal.LanceFragmentScanner;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LanceScanBuilder
    implements SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownLimit,
        SupportsPushDownOffset,
        SupportsPushDownTopN,
        SupportsPushDownAggregates {
  private static final Logger LOG = LoggerFactory.getLogger(LanceScanBuilder.class);

  private final LanceSparkReadOptions readOptions;
  private final StructType fullSchema;
  private StructType schema;

  private Filter[] pushedFilters = new Filter[0];
  private Optional<Integer> limit = Optional.empty();
  private Optional<Integer> offset = Optional.empty();
  private Optional<List<ColumnOrdering>> topNSortOrders = Optional.empty();
  private Optional<Aggregation> pushedAggregation = Optional.empty();
  private LanceLocalScan localScan = null;

  // Lazily opened dataset for reuse during scan building
  private Dataset lazyDataset = null;
  // Pinned-version read options — set in getOrOpenDataset() alongside lazyDataset. Forwarded
  // to LanceScan so executors inherit the same resolved version the driver's cache entry is
  // keyed by (Option B: driver seeds the JVM-local cache; executors hit it warm).
  private LanceSparkReadOptions lazyReadOptions = null;
  // Cache key used for release() when the driver hands lifetime off to executor checkouts.
  // Non-null only when the driver open routed through LanceDatasetCache (requires
  // dataset_cache_enabled AND a resolvable/pinned version).
  private LanceDatasetCache.CacheKey lazyCacheKey = null;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  private final java.util.Map<String, String> tableProperties;

  public LanceScanBuilder(
      StructType schema,
      LanceSparkReadOptions readOptions,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties,
      java.util.Map<String, String> tableProperties) {
    this.fullSchema = schema;
    this.schema = schema;
    this.readOptions = readOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableProperties = tableProperties != null ? tableProperties : Collections.emptyMap();
  }

  /**
   * Gets or opens a dataset for reuse during scan building. The dataset is lazily opened on first
   * access and reused for subsequent calls.
   *
   * <p>Version is always pinned on the driver — even when {@code dataset_cache_enabled=false} — so
   * executors open the same snapshot the driver planned against. Without this, disabling the cache
   * would regress snapshot isolation: each executor would resolve its own "latest" and potentially
   * see a different version than the driver did when computing stats / filter pushdown.
   *
   * <p>When {@code dataset_cache_enabled=true} (default), the driver's single open is additionally
   * routed through {@link LanceDatasetCache} so the entry is warm for every executor checkout on
   * the same JVM. Version pinning is a prerequisite for caching (the cache refuses {@code
   * version=null} keys), so both concerns share the same cheap {@link
   * Dataset#latestVersionId(String, Map)} resolve when no pin was supplied by the user.
   */
  private Dataset getOrOpenDataset() {
    if (lazyDataset != null) {
      return lazyDataset;
    }
    final LanceSparkReadOptions pinned = pinVersionIfNeeded(readOptions);
    final LanceDatasetCache.CacheKey key =
        readOptions.isDatasetCacheEnabled()
            ? LanceFragmentScanner.buildCacheKey(pinned, initialStorageOptions)
            : null;
    // Record the pinned options and (optional) cache key BEFORE the open/checkout call so that
    // if the open throws, closeLazyDataset() can release the slot correctly. lazyDataset itself
    // stays null on failure so close() is a no-op.
    lazyReadOptions = pinned;
    lazyCacheKey = key;
    lazyDataset = openPinnedDataset(pinned, key);
    return lazyDataset;
  }

  /**
   * Cheaply resolves "latest" to a concrete version id (one manifest-head GET, no full dataset
   * open) when the user did not pin a version. Version pinning is a prerequisite for both snapshot
   * isolation across driver/executor and {@link LanceDatasetCache} keying (the cache refuses {@code
   * version=null} keys).
   */
  private LanceSparkReadOptions pinVersionIfNeeded(LanceSparkReadOptions opts) {
    if (opts.getVersion() != null) {
      return opts;
    }
    Map<String, String> merged =
        LanceRuntime.mergeStorageOptions(
            opts.getStorageOptions() != null ? opts.getStorageOptions() : Collections.emptyMap(),
            initialStorageOptions);
    long resolved;
    try {
      resolved = Dataset.latestVersionId(opts.getDatasetUri(), merged);
    } catch (Error ex) {
      // Error subclasses (OOM, LinkageError, StackOverflow) are fatal and never embed URIs.
      // Re-throw unchanged so the JVM's fatal-error handling runs.
      throw ex;
    } catch (RuntimeException ex) {
      // Upstream Lance (JNI) errors embed the raw URI — including userinfo / SAS tokens /
      // signed-URL query params — into the exception message. Drop the cause and only surface
      // the class name of the underlying failure for diagnosis.
      throw LanceExceptions.wrap("resolve latest Lance dataset version", ex);
    }
    try {
      return opts.withVersion(Math.toIntExact(resolved));
    } catch (ArithmeticException ex) {
      // ArithmeticException from Math.toIntExact does NOT embed the URI, so preserving it as
      // cause here is safe and useful for diagnosis.
      throw new IllegalStateException(
          "Lance dataset version "
              + resolved
              + " exceeds int range; cannot pin for snapshot isolation",
          ex);
    }
  }

  /**
   * Opens the dataset at the pinned version, optionally routing through {@link LanceDatasetCache}
   * when {@code key != null}. On any failure the pinned side-channel state ({@code
   * lazyReadOptions}/{@code lazyCacheKey}) is cleared so a retried {@code build()} re- resolves
   * cleanly from a clean slate.
   */
  private Dataset openPinnedDataset(LanceSparkReadOptions pinned, LanceDatasetCache.CacheKey key) {
    final LanceSparkReadOptions opts = pinned;
    final Map<String, String> initial = initialStorageOptions;
    try {
      if (key != null) {
        return LanceDatasetCache.checkout(
            key, () -> Utils.openDatasetBuilder(opts).initialStorageOptions(initial).build());
      }
      return Utils.openDatasetBuilder(opts).initialStorageOptions(initial).build();
    } catch (Error ex) {
      lazyReadOptions = null;
      lazyCacheKey = null;
      throw ex;
    } catch (RuntimeException ex) {
      lazyReadOptions = null;
      lazyCacheKey = null;
      // Upstream object-store / JNI errors embed the raw URI in exception messages. Drop the
      // cause to avoid leaking credentials through Throwable.printStackTrace / Spark UI "Full
      // stacktrace" / SLF4J `{}`-with-throwable formatting.
      throw LanceExceptions.wrap("open Lance dataset", ex);
    }
  }

  /**
   * Releases the lazily opened dataset. When it was checked out from {@link LanceDatasetCache} the
   * ref-count is decremented (TTL sweep handles eviction); otherwise the dataset is closed
   * directly. Safe to call when {@code getOrOpenDataset()} failed mid-open — {@code lazyDataset}
   * will be null in that case and this method is a no-op.
   *
   * <p>This method must NEVER throw — it runs in the {@code finally} of {@link #build()} and any
   * exception here would mask the primary exception the caller threw. Instead, release/close
   * failures are logged at WARN and swallowed. The {@code lazyDataset} / {@code lazyCacheKey} /
   * {@code lazyReadOptions} fields are always cleared on exit so that a re-entrant {@code build()}
   * call (which Spark's catalyst analyzer can do) starts from a clean slate.
   */
  private void closeLazyDataset() {
    try {
      if (lazyDataset == null) {
        // Open either never ran or failed before assigning. Nothing to release.
        return;
      }
      try {
        if (lazyCacheKey != null) {
          LanceDatasetCache.release(lazyCacheKey);
        } else {
          lazyDataset.close();
        }
      } catch (RuntimeException | Error e) {
        // Must not throw from a finally path. A release/close failure here is almost always
        // benign (e.g. cache already evicted, JNI handle already invalidated); callers rely on
        // this method being no-throw so the primary build() exception propagates unmasked.
        // Drop the Throwable (do NOT pass `e` as trailing SLF4J arg): native/object-store error
        // chains embed the raw URI (userinfo, SAS tokens, signed-URL query params) in cause
        // messages. Only surface the class name for diagnosis.
        LOG.warn(
            "Failed to release lazy dataset handle (cached={}); suppressing so primary exception"
                + " is not masked (underlying={})",
            lazyCacheKey != null,
            e.getClass().getName());
      }
    } finally {
      lazyDataset = null;
      lazyCacheKey = null;
      lazyReadOptions = null;
    }
  }

  @Override
  public Scan build() {
    // Return LocalScan if we have a metadata-only aggregation result
    if (localScan != null) {
      closeLazyDataset();
      return localScan;
    }

    // Wrap the whole planning path in try/finally so that any mid-method throw
    // (manifest read, zonemap load, fragment planScan) does not leak the cache refcount
    // or the open Dataset handle. closeLazyDataset() is safe to call when nothing opened.
    try {
      // Get statistics from manifest summary before closing dataset
      ManifestSummary summary = getOrOpenDataset().getVersion().getManifestSummary();

      // Collect all columns that need zonemap stats: filter columns + partition column (if
      // declared).
      Set<String> columnsToLoad = extractReferencedColumns(pushedFilters);
      String partitionColumn = tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
      if (partitionColumn != null && !partitionColumn.trim().isEmpty()) {
        partitionColumn = partitionColumn.trim();
        columnsToLoad.add(partitionColumn);
      } else {
        partitionColumn = null;
      }

      // Load zonemap stats for all requested columns in one pass.
      Map<String, List<ZoneStats>> zonemapStats =
          loadZonemapStats(getOrOpenDataset(), columnsToLoad);

      // Detect partition-compatible columns, gated on lance.partition.columns table property.
      // Currently a partitioned column is only valid if each fragment contains only a single
      // value for that column (i.e., all zonemap zones have min == max with the same value).
      ZonemapFragmentPruner.PartitionInfo partitionInfo = null;
      if (partitionColumn != null) {
        if (!zonemapStats.containsKey(partitionColumn)) {
          LOG.warn(
              "Partition column '{}' declared in {} has no zonemap index or stats;"
                  + " partition detection disabled",
              partitionColumn,
              LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
        } else {
          Map<Integer, Comparable<?>> partValues =
              ZonemapFragmentPruner.computeFragmentPartitionValues(
                      zonemapStats.get(partitionColumn))
                  .orElse(null);
          if (partValues != null) {
            partitionInfo = new ZonemapFragmentPruner.PartitionInfo(partitionColumn, partValues);
            LOG.info(
                "Detected partition-compatible column '{}' with {} fragments",
                partitionColumn,
                partValues.size());
          }
        }
      }

      // Pre-compute fragment pruning so we can (a) estimate post-pruning statistics for
      // JoinSelection (BroadcastHashJoin vs SortMergeJoin) and (b) pass the cached result
      // to LanceScan to avoid re-computing during planInputPartitions().
      Set<Integer> survivingFragmentIds = null;
      if (pushedFilters.length > 0 && !zonemapStats.isEmpty()) {
        survivingFragmentIds =
            ZonemapFragmentPruner.pruneFragments(pushedFilters, zonemapStats).orElse(null);
      }

      // Scale rows and full size by the zonemap fragment-pruning ratio first, then let
      // LanceStatistics.estimateProjected apply the column-width ratio on top
      // (when the projected schema is narrower than the full schema).
      long projectedRows = summary.getTotalRows();
      long projectedFullSize = summary.getTotalFilesSize();
      if (survivingFragmentIds != null && summary.getTotalFragments() > 0) {
        double ratio = (double) survivingFragmentIds.size() / summary.getTotalFragments();
        projectedRows = (long) (projectedRows * ratio);
        projectedFullSize = (long) (projectedFullSize * ratio);
      }
      LanceStatistics statistics =
          LanceStatistics.estimateProjected(projectedRows, projectedFullSize, fullSchema, schema);
      if (survivingFragmentIds != null) {
        LOG.debug(
            "Scan statistics after pruning: {} of {} fragments survive,"
                + " estimatedSize={}, estimatedRows={} (full: size={}, rows={})",
            survivingFragmentIds.size(),
            summary.getTotalFragments(),
            statistics.sizeInBytes(),
            statistics.numRows(),
            summary.getTotalFilesSize(),
            summary.getTotalRows());
      }

      // Harvest fragment list + resolved version + per-fragment row counts
      // from the same Dataset used for manifest/zonemap work above, so that
      // LanceScan.planInputPartitions() does not need to reopen.
      LanceSplit.ScanPlanResult preplannedResult = LanceSplit.planScan(getOrOpenDataset());

      // Capture the pinned read options before releasing the dataset. Forwarding pinned options
      // to LanceScan ensures executor-side checkouts use the same cache key the driver just seeded.
      LanceSparkReadOptions forwardedReadOptions =
          lazyReadOptions != null ? lazyReadOptions : readOptions;

      // Ensure the cache-aware scheduling endpoint is registered on the driver so executors
      // can report cache locations back for preferredLocations hints.
      if (org.lance.spark.internal.LanceExecutorCache.isEnabled()
          && forwardedReadOptions.isDatasetCacheEnabled()) {
        try {
          org.apache.spark.sql.SparkSession session = org.apache.spark.sql.SparkSession.active();
          org.apache.spark.sql.lance.internal.LanceCacheLocationEndpoint$.MODULE$.ensureRegistered(
              session.sparkContext());
        } catch (Exception e) {
          // SparkSession not available or endpoint registration failed — cache still works,
          // just without scheduling hints.
        }
      }

      Optional<String> whereCondition =
          FilterPushDown.compileFiltersToSqlWhereClause(pushedFilters);
      return new LanceScan(
          schema,
          forwardedReadOptions,
          whereCondition,
          limit,
          offset,
          topNSortOrders,
          pushedAggregation,
          pushedFilters,
          statistics,
          zonemapStats,
          survivingFragmentIds,
          partitionInfo,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          preplannedResult);
    } finally {
      // Release the driver-side dataset handle (either cache release or direct close).
      // Runs on both success AND mid-method exception so no refcount leak.
      closeLazyDataset();
    }
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = requiredSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    if (!readOptions.isPushDownFilters()) {
      return filters;
    }
    // Pre-filter executor cache (V2): when the executor disk cache is enabled, skip all filter
    // pushdown so each fragment scan returns raw projected columns. This is what lets the cache
    // key drop the WHERE clause and share entries across queries with different predicates.
    // Note: we cannot check readOptions.getVersion() here because version pinning happens later
    // in getOrOpenDataset()/build(). The cache guard in LanceFragmentScanner.getArrowReader()
    // does the full 3-way check at execution time.
    if (org.lance.spark.internal.LanceExecutorCache.isEnabled()
        && readOptions.isDatasetCacheEnabled()) {
      pushedFilters = new Filter[0];
      return filters;
    }
    Filter[][] processFilters = FilterPushDown.processFilters(filters);
    pushedFilters = processFilters[0];
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public boolean pushLimit(int limit) {
    this.limit = Optional.of(limit);
    return true;
  }

  @Override
  public boolean pushOffset(int offset) {
    // `getOrOpenDataset()` increments a LanceDatasetCache refcount (Option B). If the body
    // below throws before `build()` runs (or if Catalyst discards this builder without ever
    // calling `build()`), `closeLazyDataset()` is never invoked and the refcount leaks for
    // the lifetime of the JVM — TTL sweep ignores entries with `refCount != 0`. Wrap the
    // whole block in try/catch and release on any throw so the refcount is balanced even on
    // pushdown failure.
    try {
      // Only one data file can be pushed down the offset.
      List<Integer> fragmentIds =
          getOrOpenDataset().getFragments().stream()
              .map(Fragment::getId)
              .collect(Collectors.toList());
      if (fragmentIds.size() == 1) {
        this.offset = Optional.of(offset);
        return true;
      } else {
        return false;
      }
    } catch (Error ex) {
      // Fatal errors still leak the cache refcount if we abandon the builder without release.
      // Attempt the release but do not mask the original Error.
      try {
        closeLazyDataset();
      } catch (Throwable suppressed) {
        ex.addSuppressed(suppressed);
      }
      throw ex;
    } catch (RuntimeException ex) {
      closeLazyDataset();
      // Re-throwing the raw RuntimeException here leaks the upstream `getMessage()` content
      // unchanged: if `getOrOpenDataset().getFragments()` surfaced a native / object-store
      // error whose message embeds the raw dataset URI (userinfo, SAS tokens, signed-URL
      // params), Catalyst's pushdown-retry and Spark UI "Full stacktrace" will both walk that
      // message and any cause chain. Funnel through LanceExceptions.wrap so the redaction
      // boundary covers the pushdown path too.
      throw LanceExceptions.wrap("enumerate Lance fragments for offset pushdown", ex);
    }
  }

  @Override
  public boolean isPartiallyPushed() {
    return true;
  }

  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    // The Order by operator will use compute thread in lance.
    // So it's better to have an option to enable it.
    if (!readOptions.isTopNPushDown()) {
      return false;
    }
    this.limit = Optional.of(limit);
    List<ColumnOrdering> topNSortOrders = new ArrayList<>();
    for (SortOrder sortOrder : orders) {
      ColumnOrdering.Builder builder = new ColumnOrdering.Builder();
      builder.setNullFirst(sortOrder.nullOrdering() == NullOrdering.NULLS_FIRST);
      builder.setAscending(sortOrder.direction() == SortDirection.ASCENDING);
      if (!(sortOrder.expression() instanceof FieldReference)) {
        return false;
      }
      FieldReference reference = (FieldReference) sortOrder.expression();
      builder.setColumnName(reference.fieldNames()[0]);
      topNSortOrders.add(builder.build());
    }
    this.topNSortOrders = Optional.of(topNSortOrders);
    return true;
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    AggregateFunc[] funcs = aggregation.aggregateExpressions();
    if (aggregation.groupByExpressions().length > 0) {
      return false;
    }
    // See `pushOffset` for why we must release the cache refcount on any throw: if
    // `getCountFromMetadata` (or any downstream call) fails after `getOrOpenDataset()`
    // incremented the refcount, `build()` may never run and the entry leaks permanently.
    try {
      if (funcs.length == 1 && funcs[0] instanceof CountStar) {
        // Check if we can use metadata-based count (no filters pushed)
        if (pushedFilters.length == 0) {
          Optional<Long> metadataCount = getCountFromMetadata(getOrOpenDataset());
          if (metadataCount.isPresent()) {
            // Create LocalScan with pre-computed count result
            StructType countSchema = new StructType().add("count", DataTypes.LongType);
            InternalRow[] rows = new InternalRow[1];
            rows[0] = new GenericInternalRow(new Object[] {metadataCount.get()});
            this.localScan = new LanceLocalScan(countSchema, rows, readOptions.getDatasetUri());
            return true;
          }
        }
        // Fall back to scan-based count (with filters or metadata unavailable)
        this.pushedAggregation = Optional.of(aggregation);
        return true;
      }

      return false;
    } catch (Error ex) {
      try {
        closeLazyDataset();
      } catch (Throwable suppressed) {
        ex.addSuppressed(suppressed);
      }
      throw ex;
    } catch (RuntimeException ex) {
      closeLazyDataset();
      // See pushOffset: upstream native errors embed the raw URI in their message.
      throw LanceExceptions.wrap("enumerate Lance fragments for aggregation pushdown", ex);
    }
  }

  private static Optional<Long> getCountFromMetadata(Dataset dataset) {
    try {
      ManifestSummary summary = dataset.getVersion().getManifestSummary();
      return Optional.of(summary.getTotalRows());
    } catch (Exception e) {
      // Fall back to a scan-based count, but log the underlying class so an operator can tell
      // a benign "older manifest version without totals" from a real issue (e.g. transient
      // object-store error) that masks a catastrophically slow full-scan count. Do NOT pass
      // the throwable or e.getMessage() — SLF4J prints the full Throwable chain, and upstream
      // object-store errors embed the raw URI (userinfo / SAS tokens / signed-URL params).
      LOG.debug(
          "Metadata count unavailable, falling back to scan-based count (underlying={})",
          e.getClass().getName());
      return Optional.empty();
    }
  }

  /**
   * Loads zonemap statistics for the requested columns. Only loads stats for columns that have a
   * zonemap index.
   */
  private Map<String, List<ZoneStats>> loadZonemapStats(Dataset dataset, Set<String> columns) {
    if (columns.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<String> zonemapColumns = findZonemapIndexedColumns(dataset);
    if (zonemapColumns.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, List<ZoneStats>> result = new HashMap<>();
    for (String col : columns) {
      if (zonemapColumns.contains(col)) {
        try {
          List<ZoneStats> stats = dataset.getZonemapStats(col);
          if (!stats.isEmpty()) {
            result.put(col, stats);
            LOG.debug("Loaded {} zonemap zones for column '{}'", stats.size(), col);
          }
        } catch (Exception e) {
          // Do NOT log e.getMessage() — upstream Lance/object-store errors embed the raw URI
          // (including userinfo / SAS tokens / signed-URL query params) into their message.
          // Log only the exception class name so an operator can distinguish a missing-index
          // case from a transient object-store error without leaking credentials.
          LOG.debug(
              "Failed to load zonemap stats for column '{}': underlying={}",
              col,
              e.getClass().getName());
        }
      }
    }

    if (!result.isEmpty()) {
      LOG.debug("Loaded zonemap stats for {} columns: {}", result.size(), result.keySet());
    }

    return result;
  }

  private Set<String> findZonemapIndexedColumns(Dataset dataset) {
    Set<String> columns = new HashSet<>();
    try {
      Map<Integer, String> fieldIdToName = new HashMap<>();
      for (LanceField field : dataset.getLanceSchema().fields()) {
        fieldIdToName.put(field.getId(), field.getName());
      }

      // Use the criteria-based overload so that indexes missing index_details
      // (created by older versions) are silently skipped instead of causing errors.
      IndexCriteria criteria = new IndexCriteria.Builder().build();
      for (IndexDescription idx : dataset.describeIndices(criteria)) {
        if ("ZONEMAP".equalsIgnoreCase(idx.getIndexType())) {
          for (int fieldId : idx.getFieldIds()) {
            String name = fieldIdToName.get(fieldId);
            if (name != null) {
              columns.add(name);
            }
          }
        }
      }
    } catch (Exception e) {
      // Do NOT log e.getMessage() — upstream object-store errors embed the raw URI with
      // credentials; surface only the exception class name.
      LOG.warn("Failed to query zonemap indexes: underlying={}", e.getClass().getName());
    }
    return columns;
  }

  private static Set<String> extractReferencedColumns(Filter[] filters) {
    Set<String> columns = new HashSet<>();
    for (Filter filter : filters) {
      for (String attr : filter.references()) {
        columns.add(attr);
      }
    }
    return columns;
  }
}
