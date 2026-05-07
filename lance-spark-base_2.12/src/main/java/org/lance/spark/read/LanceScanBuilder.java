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
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
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
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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

  // Lazily opened dataset for reuse during scan building. volatile + DCL in getOrOpenDataset
  // protects against concurrent push* calls from the optimizer (no documented thread-safety
  // guarantee on ScanBuilder) that could otherwise leak a native handle.
  private volatile Dataset lazyDataset = null;

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

  /** Lazy + DCL: reuses the dataset across push/build calls without leaking a native handle. */
  private Dataset getOrOpenDataset() {
    Dataset local = lazyDataset;
    if (local == null) {
      synchronized (this) {
        local = lazyDataset;
        if (local == null) {
          local = Utils.openDatasetBuilder(readOptions).build();
          lazyDataset = local;
        }
      }
    }
    return local;
  }

  /** Closes the lazy dataset if opened. Short-circuits without locking when never opened. */
  private void closeLazyDataset() {
    if (lazyDataset == null) {
      return;
    }
    synchronized (this) {
      if (lazyDataset != null) {
        lazyDataset.close();
        lazyDataset = null;
      }
    }
  }

  @Override
  public Scan build() {
    // Wrap the entire body in try/finally so any exception (manifest read, zonemap I/O, JNI,
    // SparkSession config access) still closes the lazily-opened native dataset. Without this
    // the failure path silently leaks a Lance Dataset native handle on every failed build().
    try {
      return buildInternal();
    } finally {
      closeLazyDataset();
    }
  }

  private Scan buildInternal() {
    // Return LocalScan if we have a metadata-only aggregation result
    if (localScan != null) {
      return localScan;
    }

    // Get statistics from manifest summary before closing dataset
    org.lance.Version datasetVersion = getOrOpenDataset().getVersion();
    ManifestSummary summary = datasetVersion.getManifestSummary();
    long currentManifestVersion = datasetVersion.getId();

    // Resolve once per scan: SparkSession.active() walks the active-session ThreadLocal each
    // call, so hoisting avoids three lookups (kill-switch, max-columns, allowStale) on every
    // build(). Null when no session is active (e.g. unit tests); resolvers fall through to
    // per-scan options in that case.
    SparkSession session = activeSparkSessionOrNull();

    // Collect all columns that need zonemap stats: filter columns + partition column (if declared)
    // + projected columns (for Phase 1 column-stats reporting). The cap on projected columns
    // bounds driver-side I/O / memory; filter+partition columns are always loaded since they
    // already drive fragment pruning.
    Set<String> columnsToLoad = new LinkedHashSet<>(extractReferencedColumns(pushedFilters));
    String partitionColumn = tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
    if (partitionColumn != null && !partitionColumn.trim().isEmpty()) {
      partitionColumn = partitionColumn.trim();
      columnsToLoad.add(partitionColumn);
    } else {
      partitionColumn = null;
    }
    boolean cboColumnStatsEnabled = resolveCboColumnStatsEnabled(session, readOptions);
    if (cboColumnStatsEnabled) {
      int cap = resolveCboColumnStatsMaxColumns(session, readOptions);
      for (org.apache.spark.sql.types.StructField f : schema.fields()) {
        if (columnsToLoad.size() >= cap) {
          break;
        }
        columnsToLoad.add(f.name());
      }
    }

    // Load zonemap stats for all requested columns in one pass.
    Map<String, List<ZoneStats>> zonemapStats = loadZonemapStats(getOrOpenDataset(), columnsToLoad);

    // Detect partition-compatible columns, gated on lance.partition.columns table property.
    // Currently a partitioned column is only valid if each fragment contains only a single
    // value for that column (i.e., all zonemap zones have min == max with the same value).
    ZonemapFragmentPruner.PartitionInfo partitionInfo = null;
    if (partitionColumn != null) {
      if (!zonemapStats.containsKey(partitionColumn)) {
        LOG.warn(
            "Partition column '{}' declared in {} has no zonemap index or stats;"
                + " partition detection disabled",
            sanitizeForLog(partitionColumn),
            LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
      } else {
        Map<Integer, Comparable<?>> partValues =
            ZonemapFragmentPruner.computeFragmentPartitionValues(zonemapStats.get(partitionColumn))
                .orElse(null);
        if (partValues != null) {
          partitionInfo = new ZonemapFragmentPruner.PartitionInfo(partitionColumn, partValues);
          LOG.info(
              "Detected partition-compatible column '{}' with {} fragments",
              sanitizeForLog(partitionColumn),
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
    Map<NamedReference, ColumnStatistics> aggregatedColumnStats;
    if (!cboColumnStatsEnabled) {
      aggregatedColumnStats = Collections.emptyMap();
    } else {
      // Persistent stats fast path: if ANALYZE TABLE has been run and the manifest version matches,
      // build the column-stats map directly from TBLPROPERTIES — O(1) per column with zero zonemap
      // I/O. The aggregation fallback runs only when stats are absent, stale, or explicitly skipped
      // via the per-scan kill-switch.
      Map<NamedReference, ColumnStatistics> persistedStats =
          loadPersistedColumnStats(
              session, tableProperties, fullSchema, schema, currentManifestVersion);
      if (persistedStats != null) {
        aggregatedColumnStats = persistedStats;
        // Info-level confirms the fast path is active without requiring debug logging — the
        // live-aggregation fall-back paths below also log at info, so parity makes it
        // possible to diagnose CBO performance regressions from production logs alone.
        LOG.info(
            "Using persisted column stats (ANALYZE fast path) for {} columns; "
                + "skipping zonemap aggregation",
            persistedStats.size());
      } else {
        aggregatedColumnStats = aggregateProjectedColumnStats(zonemapStats, schema);
      }
    }

    LanceStatistics statistics =
        LanceStatistics.estimateProjected(
            projectedRows, projectedFullSize, fullSchema, schema, aggregatedColumnStats);
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

    // Dataset close is handled by build()'s try/finally — no explicit close needed here.

    Optional<String> whereCondition = FilterPushDown.compileFiltersToSqlWhereClause(pushedFilters);
    return new LanceScan(
        schema,
        readOptions,
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
        namespaceProperties);
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
    Filter[][] processFilters = FilterPushDown.processFilters(filters);
    pushedFilters = processFilters[0];
    return processFilters[1];
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
  }

  private static Optional<Long> getCountFromMetadata(Dataset dataset) {
    try {
      ManifestSummary summary = dataset.getVersion().getManifestSummary();
      return Optional.of(summary.getTotalRows());
    } catch (Exception e) {
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
          // Sanitize: column name comes from TBLPROPERTIES path or filter expressions; the
          // exception message can echo native error strings that may include user data.
          LOG.debug(
              "Failed to load zonemap stats for column '{}': {}",
              sanitizeForLog(col),
              sanitizeForLog(e.getMessage()));
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
      // Accept both BTREE and ZONEMAP indexes — Lance's btree implementation embeds
      // a zonemap, so getZonemapStats() returns valid per-zone stats for either type.
      // Loading later filters out columns whose stats are empty, so over-including
      // is cheap.
      IndexCriteria criteria = new IndexCriteria.Builder().build();
      for (IndexDescription idx : dataset.describeIndices(criteria)) {
        String type = idx.getIndexType();
        if (type == null) {
          continue;
        }
        if ("ZONEMAP".equalsIgnoreCase(type) || "BTREE".equalsIgnoreCase(type)) {
          for (int fieldId : idx.getFieldIds()) {
            String name = fieldIdToName.get(fieldId);
            if (name != null) {
              columns.add(name);
            }
          }
        }
      }
    } catch (Exception e) {
      // Sanitize: a JNI exception message may echo path components or property values.
      LOG.warn("Failed to query zonemap indexes: {}", sanitizeForLog(e.getMessage()));
    }
    return columns;
  }

  /**
   * Best-effort lookup of the active {@link SparkSession}. Returns {@code null} when no session is
   * bound to the current thread (e.g. unit tests that exercise the planner directly). The three
   * SparkConf-aware resolvers all gate on a non-null session, falling through to per-scan options
   * when null — this is the same fallback shape as the previous per-method try/catch, just hoisted
   * to a single ThreadLocal lookup per scan.
   */
  private static SparkSession activeSparkSessionOrNull() {
    try {
      return SparkSession.active();
    } catch (Exception ignored) {
      return null;
    }
  }

  /**
   * Resolve the column-stats kill-switch using two-level lookup. The SparkConf key {@link
   * LanceStatsKeys#SPARK_CONF_CBO_COLUMN_STATS_ENABLED} acts as a global kill-switch: when set, it
   * overrides the per-scan {@link LanceSparkReadOptions#isCboColumnStatsEnabled()} value. When
   * unset, the per-scan option (default {@code true}) wins. This makes a single session-level
   * config able to disable the feature everywhere for safe rollback.
   */
  private static boolean resolveCboColumnStatsEnabled(
      SparkSession session, LanceSparkReadOptions readOptions) {
    if (session != null) {
      try {
        String key = LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ENABLED;
        if (session.conf().contains(key)) {
          return Boolean.parseBoolean(session.conf().get(key));
        }
      } catch (Exception ignored) {
        // Conf access can throw if the session was closed mid-scan — fall through.
      }
    }
    return readOptions.isCboColumnStatsEnabled();
  }

  /**
   * Resolve the column-stats max-columns cap. SparkConf {@link
   * LanceStatsKeys#SPARK_CONF_CBO_COLUMN_STATS_MAX_COLUMNS} overrides the per-scan value when set,
   * mirroring the {@link #resolveCboColumnStatsEnabled} pattern. Used to cap driver-side I/O —
   * loading zonemap stats for many columns is the dominant per-scan cost and the primary regression
   * source on wide-projection queries.
   */
  private static int resolveCboColumnStatsMaxColumns(
      SparkSession session, LanceSparkReadOptions readOptions) {
    if (session != null) {
      try {
        String key = LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_MAX_COLUMNS;
        if (session.conf().contains(key)) {
          String raw = session.conf().get(key);
          try {
            int parsed = Integer.parseInt(raw);
            if (parsed >= 0) {
              return parsed;
            }
            LOG.warn(
                "{}={} is negative; using per-scan default {}",
                key,
                raw,
                readOptions.getCboColumnStatsMaxColumns());
          } catch (NumberFormatException nfe) {
            LOG.warn(
                "{}={} is not a valid integer; using per-scan default {}",
                key,
                raw,
                readOptions.getCboColumnStatsMaxColumns());
          }
        }
      } catch (Exception ignored) {
        // Conf access can throw if the session was closed mid-scan — fall through.
      }
    }
    return readOptions.getCboColumnStatsMaxColumns();
  }

  /**
   * Strip CR/LF/tab from a string before passing it to SLF4J. TBLPROPERTIES values are
   * user-controllable (any actor with ALTER TABLE permission can write them) and SLF4J's {}
   * interpolation does not strip control characters, so an attacker could otherwise inject fake log
   * lines or ANSI sequences. Truncates beyond a sane length to bound log volume.
   */
  private static String sanitizeForLog(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.length() > 256 ? value.substring(0, 256) + "…" : value;
    return trimmed.replace('\r', '_').replace('\n', '_').replace('\t', '_');
  }

  /**
   * Build {@link ColumnStatistics} for projected columns from TBLPROPERTIES populated by an earlier
   * {@code ANALYZE TABLE … COMPUTE STATISTICS} run. Returns {@code null} (caller falls back to live
   * zonemap aggregation) when:
   *
   * <ul>
   *   <li>No {@code lance.stats.computedAtVersion} is present (writer didn't tag a version).
   *   <li>The recorded version differs from the current manifest version (data has been written
   *       since {@code ANALYZE} unless {@code spark.lance.cbo.column.stats.allow.stale=true}).
   *   <li>Decoding errors out — better to silently re-aggregate than report bad stats to CBO.
   *   <li>None of the projected columns has any persisted stats (e.g., ANALYZE was run on a
   *       different column set than the current scan projects). Returning {@code null} causes the
   *       caller to fall back to live zonemap aggregation, which may still find stats for the
   *       projected columns via their zonemap indexes.
   * </ul>
   */
  // Package-private for unit-test coverage of the fast-path branches (complete sentinel,
  // formatVersion gate, schemaHash drift guard, allowStale bypass, all-null skip).
  static Map<NamedReference, ColumnStatistics> loadPersistedColumnStats(
      SparkSession session,
      java.util.Map<String, String> tableProperties,
      StructType fullSchema,
      StructType projectedSchema,
      long currentManifestVersion) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return null;
    }
    // Atomicity sentinel: ANALYZE writes lance.stats.complete=true LAST, so absence indicates
    // a partial / interrupted write. Refusing to use partial stats is safer than reporting them.
    String complete = tableProperties.get(LanceStatsKeys.COMPLETE);
    if (!"true".equalsIgnoreCase(complete)) {
      return null;
    }
    // Format-version gate: only consume payloads we know how to decode. Missing version is
    // fail-safe (skip); unrecognized version warns and skips so a future v2 writer can ship
    // before the readers catch up.
    String formatVersion = tableProperties.get(LanceStatsKeys.VERSION);
    if (formatVersion == null) {
      LOG.debug(
          "lance.stats.version key absent — falling back to live aggregation (complete=true was "
              + "set without a version, suggesting a hand-edited or partially-written payload)");
      return null;
    }
    if (!LanceStatsKeys.SUPPORTED_VERSION.equals(formatVersion.trim())) {
      // Sanitize: TBLPROPERTIES values are user-controllable; SLF4J's {} passes CR/LF verbatim.
      LOG.warn(
          "Unrecognized lance.stats.version='{}' — falling back to live aggregation. "
              + "This usually means the table was analyzed with a newer connector; upgrade the "
              + "connector or re-run ANALYZE TABLE with the current connector to restore the fast "
              + "path.",
          sanitizeForLog(formatVersion));
      return null;
    }
    String versionStr = tableProperties.get(LanceStatsKeys.COMPUTED_AT_VERSION);
    if (versionStr == null) {
      return null;
    }
    long computedAtVersion;
    try {
      computedAtVersion = Long.parseLong(versionStr.trim());
    } catch (NumberFormatException e) {
      LOG.warn(
          "Malformed lance.stats.computedAtVersion='{}' — ignoring persisted stats",
          sanitizeForLog(versionStr));
      return null;
    }
    if (computedAtVersion < 0) {
      LOG.warn(
          "Negative lance.stats.computedAtVersion={} — ignoring persisted stats",
          computedAtVersion);
      return null;
    }
    boolean allowStale = false;
    if (session != null) {
      try {
        String key = LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE;
        if (session.conf().contains(key)) {
          allowStale = Boolean.parseBoolean(session.conf().get(key));
        }
      } catch (Exception ignored) {
        // Conf access can throw if the session was closed mid-scan — keep allowStale=false.
      }
    }
    // Schema fingerprint guard: a hash mismatch means columns were renamed/retyped/dropped/
    // reordered, and persisted stats may attribute values to the wrong column. Missing hash is
    // also fail-safe — a hand-edited or pre-hash payload can't be verified.
    String recordedSchemaHash = tableProperties.get(LanceStatsKeys.SCHEMA_HASH);
    if (recordedSchemaHash == null) {
      // DEBUG-level: a missing hash fires per build() for legacy tables and would flood logs
      // under AQE re-planning. Hash MISMATCH (a real schema-drift signal) stays at INFO below.
      LOG.debug(
          "lance.stats.schemaHash key absent — falling back to live aggregation. Persisted stats "
              + "predate the schema-drift guard; re-run ANALYZE TABLE to populate the hash.");
      return null;
    }
    String currentSchemaHash = LanceStatsKeys.computeSchemaHash(fullSchema);
    if (!recordedSchemaHash.equals(currentSchemaHash)) {
      // Sanitize the recorded hash — a poisoned property could violate the SHA-256-hex shape.
      LOG.info(
          "lance.stats.schemaHash mismatch (recorded={}, current={}); schema has changed since "
              + "ANALYZE — falling back to live aggregation. Re-run ANALYZE TABLE to refresh.",
          sanitizeForLog(recordedSchemaHash.substring(0, Math.min(8, recordedSchemaHash.length()))),
          currentSchemaHash.substring(0, Math.min(8, currentSchemaHash.length())));
      return null;
    }

    if (computedAtVersion != currentManifestVersion && !allowStale) {
      // DEBUG-level: under AQE re-planning this fires once per build(), so a busy workload
      // against a recently-INSERTed table would emit thousands of INFO lines per minute and
      // drown out genuine signals. Operators wanting visibility can enable DEBUG.
      LOG.debug(
          "Persisted column stats are stale (computedAtVersion={}, currentVersion={}); "
              + "falling back to live zonemap aggregation. Re-run ANALYZE TABLE to refresh, "
              + "or set {}=true.",
          computedAtVersion,
          currentManifestVersion,
          LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_ALLOW_STALE);
      return null;
    }

    Map<NamedReference, ColumnStatistics> result = new LinkedHashMap<>();
    for (org.apache.spark.sql.types.StructField f : projectedSchema.fields()) {
      String keyBase = LanceStatsKeys.COLUMN_PREFIX + f.name();
      String minStr = tableProperties.get(keyBase + LanceStatsKeys.COLUMN_SUFFIX_MIN);
      String maxStr = tableProperties.get(keyBase + LanceStatsKeys.COLUMN_SUFFIX_MAX);
      String nullStr = tableProperties.get(keyBase + LanceStatsKeys.COLUMN_SUFFIX_NULL_COUNT);
      String ndvStr = tableProperties.get(keyBase + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_COUNT);
      String avgLenStr = tableProperties.get(keyBase + LanceStatsKeys.COLUMN_SUFFIX_AVG_LEN);
      String maxLenStr = tableProperties.get(keyBase + LanceStatsKeys.COLUMN_SUFFIX_MAX_LEN);
      // Note: keyBase + ".distinctMode" ("approx" / "exact") is intentionally read by humans /
      // diagnostics only. Spark's ColumnStatistics interface has no field to communicate
      // approximate-vs-exact NDV provenance, so the read path doesn't currently surface it.
      // The write side (LanceAnalyzeTableExec) persists it for inspection and so the orphan-
      // cleanup regex can collect it when columns are dropped.
      // .histogram / .histogramFormat are intentionally not decoded: PersistedColumnStatistics
      // returns empty for histogram() until the V2 CBO path is wired, so decoding base64 on
      // every scan would be pure overhead. The writer still reserves the wire-format slot.
      if (minStr == null && maxStr == null && nullStr == null && ndvStr == null
          && avgLenStr == null && maxLenStr == null) {
        continue; // column not analyzed
      }
      Object minVal = null;
      Object maxVal = null;
      long nullCount;
      Long ndv;
      Long avgLen;
      Long maxLen;
      try {
        if (minStr != null) {
          minVal = ColumnStatsCodec.decode(minStr);
        }
        if (maxStr != null) {
          maxVal = ColumnStatsCodec.decode(maxStr);
        }
        nullCount = nullStr == null ? 0L : Long.parseLong(nullStr);
        ndv = ndvStr == null ? null : Long.parseLong(ndvStr);
        avgLen = avgLenStr == null ? null : Long.parseLong(avgLenStr);
        maxLen = maxLenStr == null ? null : Long.parseLong(maxLenStr);
        // Range-validate per the CBO selectivity contract: nullCount >= 0 and distinctCount > 0.
        // Otherwise FilterEstimation produces selectivity outside [0, 1].
        if (nullCount < 0L) {
          throw new NumberFormatException("nullCount must be non-negative, got " + nullCount);
        }
        if (ndv != null && ndv <= 0L) {
          throw new NumberFormatException("distinctCount must be positive, got " + ndv);
        }
        if (avgLen != null && avgLen < 0L) {
          throw new NumberFormatException("avgLen must be non-negative, got " + avgLen);
        }
        if (maxLen != null && maxLen < 0L) {
          throw new NumberFormatException("maxLen must be non-negative, got " + maxLen);
        }
      } catch (Exception e) {
        // Skip the column rather than fail the whole scan. NumberFormatException's message
        // echoes the offending input verbatim — sanitize before logging in case the value
        // carries CR/LF.
        LOG.warn(
            "Failed to decode persisted stats for column {}: {} — skipping",
            sanitizeForLog(f.name()),
            sanitizeForLog(e.getMessage()));
        continue;
      }
      // Skip the column when any persisted bound decoded to null (oversized body, unknown
      // tag, etc.) and there's no NDV to fall back on. Handing Spark a ColumnStatistics with
      // one missing bound is worse than reporting no stats — the CBO would treat the missing
      // side as unbounded, biasing selectivity.
      boolean minLost = minStr != null && minVal == null;
      boolean maxLost = maxStr != null && maxVal == null;
      if ((minLost || maxLost) && ndv == null) {
        // DataType in the message helps narrow failures to a specific codec path
        // (e.g. Decimal precision overflow vs. oversized String base64).
        String sanitizedName = sanitizeForLog(f.name());
        LOG.warn(
            "Persisted stats for column {} (type={}) have a bound that decoded to null — "
                + "skipping to avoid misleading CBO; check TBLPROPERTIES for malformed "
                + "lance.stats.column.{}.* keys.",
            sanitizedName,
            f.dataType().simpleString(),
            sanitizedName);
        continue;
      }
      result.put(
          FieldReference.column(f.name()),
          new PersistedColumnStatistics(minVal, maxVal, nullCount, ndv, avgLen, maxLen));
    }
    return result.isEmpty() ? null : result;
  }

  /**
   * Trivial {@link ColumnStatistics} backed by decoded TBLPROPERTIES values.
   *
   * <p>The {@code min} / {@code max} fields hold whatever JVM type {@link ColumnStatsCodec#decode}
   * produces: {@code Integer} / {@code Long} / {@code Float} / {@code Double} / {@code Short} /
   * {@code Byte} / {@code Boolean} / {@code String} / {@code byte[]} / {@code java.math.BigDecimal}
   * for the in-tree types, plus the catalyst-internal {@code int} (DateType days-since-epoch) and
   * {@code long} (TimestampType / TimestampNTZType micros-since-epoch). Spark's CBO calls {@code
   * CatalystTypeConverters} on these values when comparing against predicate constants, so the JVM
   * types must match what the converter accepts for each {@code DataType}.
   */
  private static final class PersistedColumnStatistics
      implements ColumnStatistics, java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final Object min;
    private final Object max;
    private final long nullCount;
    private final Long distinctCount;
    private final Long avgLen;
    private final Long maxLen;

    PersistedColumnStatistics(
        Object min,
        Object max,
        long nullCount,
        Long distinctCount,
        Long avgLen,
        Long maxLen) {
      this.min = min;
      this.max = max;
      this.nullCount = nullCount;
      this.distinctCount = distinctCount;
      this.avgLen = avgLen;
      this.maxLen = maxLen;
    }

    @Override
    public java.util.OptionalLong distinctCount() {
      return distinctCount == null
          ? java.util.OptionalLong.empty()
          : java.util.OptionalLong.of(distinctCount);
    }

    @Override
    public java.util.Optional<Object> min() {
      return min == null ? java.util.Optional.empty() : java.util.Optional.of(min);
    }

    @Override
    public java.util.Optional<Object> max() {
      return max == null ? java.util.Optional.empty() : java.util.Optional.of(max);
    }

    @Override
    public java.util.OptionalLong nullCount() {
      return java.util.OptionalLong.of(nullCount);
    }

    @Override
    public java.util.OptionalLong avgLen() {
      return avgLen == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(avgLen);
    }

    @Override
    public java.util.OptionalLong maxLen() {
      return maxLen == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(maxLen);
    }

    // Histogram intentionally left empty: see the note in loadPersistedColumnStats about why
    // we don't decode .histogram on the read path.
    @Override
    public java.util.Optional<org.apache.spark.sql.connector.read.colstats.Histogram> histogram() {
      return java.util.Optional.empty();
    }
  }

  /**
   * Aggregate per-column zonemap stats into Spark DSv2 {@link ColumnStatistics} keyed by {@link
   * NamedReference}. Restricted to columns that appear in the projected schema — Spark would ignore
   * stats for non-projected columns anyway, and including them risks exposing predicate- only
   * columns to optimizer rules that don't expect them.
   */
  private static Map<NamedReference, ColumnStatistics> aggregateProjectedColumnStats(
      Map<String, List<ZoneStats>> zonemapStats, StructType projectedSchema) {
    if (zonemapStats == null || zonemapStats.isEmpty()) {
      return Collections.emptyMap();
    }
    Set<String> projected = new HashSet<>();
    for (org.apache.spark.sql.types.StructField f : projectedSchema.fields()) {
      projected.add(f.name());
    }
    Map<NamedReference, ColumnStatistics> result = new LinkedHashMap<>();
    for (Map.Entry<String, List<ZoneStats>> e : zonemapStats.entrySet()) {
      if (!projected.contains(e.getKey())) {
        continue;
      }
      ColumnStatsAggregator.aggregate(e.getValue())
          .ifPresent(stats -> result.put(FieldReference.column(e.getKey()), stats));
    }
    if (!result.isEmpty()) {
      LOG.debug("Reporting column stats for {} columns: {}", result.size(), result.keySet());
    }
    return result;
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
