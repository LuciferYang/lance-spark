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
package org.lance.spark.internal;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.Utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LanceFragmentScanner implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(LanceFragmentScanner.class);

  private final Dataset dataset;
  private final LanceScanner scanner;
  private final int fragmentId;
  private final boolean withFragemtId;
  private final LanceInputPartition inputPartition;
  private final LanceDatasetCache.CacheKey cacheKey;

  private LanceFragmentScanner(
      Dataset dataset,
      LanceScanner scanner,
      int fragmentId,
      boolean withFragmentId,
      LanceInputPartition inputPartition,
      LanceDatasetCache.CacheKey cacheKey) {
    this.dataset = dataset;
    this.scanner = scanner;
    this.fragmentId = fragmentId;
    this.withFragemtId = withFragmentId;
    this.inputPartition = inputPartition;
    this.cacheKey = cacheKey;
  }

  public static LanceFragmentScanner create(int fragmentId, LanceInputPartition inputPartition) {
    Dataset dataset = null;
    LanceDatasetCache.CacheKey cacheKey = null;
    try {
      LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
      // Optionally rebuild the namespace client on the executor so the dataset open routes through
      // Utils.OpenDatasetBuilder's namespaceClient branch. This preserves the storage options
      // provider on the Rust side, which refreshes short-lived vended credentials (e.g. STS
      // tokens) during long-running scans. The price is an eager describeTable() RPC against the
      // namespace on every fragment open.
      //
      // For catalogs whose backing service authenticates per-call (e.g. Hive Metastore over
      // Kerberos) executors typically lack a TGT and that RPC fails with "GSS initiate failed".
      // Setting LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH=false makes executors
      // skip the rebuild and open the dataset by URI using the initialStorageOptions the driver
      // already obtained, at the cost of losing the Rust-side credential refresh callback.
      //
      // IMPORTANT: use withNamespace() to build a local copy rather than mutating the shared
      // readOptions instance. Under Spark speculative execution the same InputPartition (and
      // therefore the same readOptions reference) can be handed to multiple task attempts
      // concurrently; a prior `readOptions.setNamespace(...); ...; readOptions.setNamespace(null)`
      // pattern was racy and could leave a sibling attempt observing an unexpected namespace
      // state. The local copy is stack-local to this scanner and never published.
      if (inputPartition.getNamespaceImpl() != null && readOptions.isExecutorCredentialRefresh()) {
        if (LanceRuntime.useNamespaceOnWorkers(inputPartition.getNamespaceImpl())) {
          readOptions =
              readOptions.withNamespace(
                  LanceRuntime.getOrCreateNamespace(
                      inputPartition.getNamespaceImpl(), inputPartition.getNamespaceProperties()));
        } else {
          readOptions = readOptions.withNamespace(null);
        }
      }
      // Use JVM-local Dataset cache when version is pinned (driver resolves & propagates
      // version via LanceSplit.planScan → LanceInputPartition.readOptions). Cache hit skips
      // the LIST/HEAD/manifest-GET on repeated fragment opens in the same executor JVM.
      // Gated off per-query via dataset_cache_enabled=false.
      boolean cacheable = readOptions.isDatasetCacheEnabled() && readOptions.getVersion() != null;
      if (cacheable) {
        cacheKey = buildCacheKey(readOptions, inputPartition.getInitialStorageOptions());
        final LanceSparkReadOptions opts = readOptions;
        final Map<String, String> initialStorage = inputPartition.getInitialStorageOptions();
        dataset =
            LanceDatasetCache.checkout(
                cacheKey,
                () -> Utils.openDatasetBuilder(opts).initialStorageOptions(initialStorage).build());
      } else {
        dataset =
            Utils.openDatasetBuilder(readOptions)
                .initialStorageOptions(inputPartition.getInitialStorageOptions())
                .build();
      }
      Fragment fragment = dataset.getFragment(fragmentId);
      if (fragment == null) {
        // FragmentNotFoundException carries no URI — only fragmentId + version — so it is safe
        // to surface directly to operators. The outer catch block detects this marker subclass
        // and rethrows it unchanged, bypassing LanceExceptions.wrap's generic wrapper (which
        // would otherwise replace this actionable diagnostic with a URI-redacted placeholder).
        throw new FragmentNotFoundException(fragmentId, readOptions.getVersion());
      }
      ScanOptions.Builder scanOptions = new ScanOptions.Builder();
      List<String> projectedColumns = getColumnNames(inputPartition.getSchema());
      if (projectedColumns.isEmpty() && inputPartition.getSchema().isEmpty()) {
        // Lance requires at least one projected column. Use _rowid as a lightweight
        // sentinel so the scanner still returns the correct row count (e.g. SELECT 1).
        // Only do this when the schema is truly empty; when the schema contains virtual
        // columns (e.g. _fragid, blob position/size) that are not passed to the scanner
        // but added later by the batch scanner, adding _rowid here would shift column
        // indices and cause Spark to read wrong data.
        scanOptions.withRowId(true);
      }
      scanOptions.columns(projectedColumns);
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptions.filter(inputPartition.getWhereCondition().get());
      }
      scanOptions.batchSize(readOptions.getBatchSize());
      scanOptions.batchReadahead(readOptions.getBatchReadahead());
      LOG.debug(
          "LanceFragmentScanner.create: fragmentId={} uri={} batchSize={} "
              + "batchReadahead={} projectedCols={} hasFilter={} hasLimit={}",
          fragmentId,
          LanceExceptions.redactUri(readOptions.getDatasetUri()),
          readOptions.getBatchSize(),
          readOptions.getBatchReadahead(),
          projectedColumns.size(),
          inputPartition.getWhereCondition().isPresent(),
          inputPartition.getLimit().isPresent());
      if (readOptions.getNearest() != null) {
        scanOptions.nearest(readOptions.getNearest());
        // We strictly set `prefilter = true` here to ensure query correctness.
        // This is necessary due to the combination of two factors:
        // 1. Spark currently performs the vector search by individually scanning each fragment.
        // 2. Lance mandates that `prefilter` must be enabled for fragmented vector queries.
        // If Spark's execution model or Lance's search functionality changes in the future,
        // we need to revisit this.
        scanOptions.prefilter(true);
      }
      if (inputPartition.getLimit().isPresent()) {
        scanOptions.limit(inputPartition.getLimit().get());
      }
      if (inputPartition.getOffset().isPresent()) {
        scanOptions.offset(inputPartition.getOffset().get());
      }
      if (inputPartition.getTopNSortOrders().isPresent()) {
        scanOptions.setColumnOrderings(inputPartition.getTopNSortOrders().get());
      }
      boolean withFragmentId =
          inputPartition.getSchema().getFieldIndex(LanceConstant.FRAGMENT_ID).nonEmpty();
      return new LanceFragmentScanner(
          dataset,
          fragment.newScan(scanOptions.build()),
          fragmentId,
          withFragmentId,
          inputPartition,
          cacheKey);
    } catch (Throwable throwable) {
      if (dataset != null) {
        try {
          if (cacheKey != null) {
            LanceDatasetCache.release(cacheKey);
          } else {
            dataset.close();
          }
        } catch (Throwable closeError) {
          throwable.addSuppressed(closeError);
        }
      }
      // Executor-side redaction boundary: upstream JNI / object-store / scan-builder errors embed
      // the raw URI (userinfo, SAS tokens, signed-URL params) into their message. Re-throwing
      // with `new RuntimeException(throwable)` preserves the original as `cause`, which Spark's
      // executor logs and the driver-side task-failure path will walk to render each message.
      // Drop the cause for RuntimeException (class name only); re-throw Error unchanged.
      if (throwable instanceof Error) {
        throw (Error) throwable;
      }
      // FragmentNotFoundException is our own safely-phrased diagnostic (fragmentId + version,
      // no URI); rethrow unchanged so operators see the actionable message instead of the
      // generic LanceExceptions.wrap placeholder.
      if (throwable instanceof FragmentNotFoundException) {
        throw (FragmentNotFoundException) throwable;
      }
      throw LanceExceptions.wrap("create Lance fragment scanner", throwable);
    }
  }

  /**
   * Raised when a requested fragment id is absent from the opened Dataset — usually indicates
   * planner/executor drift (fragments deleted between plan and scan, or stale plan cached across a
   * compaction). Message is constructed locally from {@code fragmentId} and {@code version} only
   * and carries no URI content, so {@link LanceExceptions#wrap} rethrows it unchanged rather than
   * replacing it with the generic redacted placeholder.
   */
  public static final class FragmentNotFoundException extends IllegalStateException {
    public FragmentNotFoundException(int fragmentId, Integer version) {
      super(String.format("Fragment %d not found in dataset (version=%s)", fragmentId, version));
    }
  }

  /**
   * Set of {@code LanceSparkReadOptions} config keys that identify the connector's own options
   * (rather than object-store storage options). These are stored in {@code
   * LanceSparkReadOptions.storageOptions} because Spark's DataSource API funnels every option
   * through one map, but they must not contribute to the {@link LanceDatasetCache.CacheKey} — two
   * queries differing only by {@code batch_size} or {@code dataset_cache_enabled} open the exact
   * same underlying Dataset and should share a cache slot.
   *
   * <p>Object-store credentials and region/endpoint settings (e.g. {@code access_key_id}, {@code
   * aws_region}, {@code endpoint}) are NOT in this set and continue to participate in the cache key
   * — two queries with different credentials must never share a Dataset handle.
   */
  private static final java.util.Set<String> CONNECTOR_OWNED_OPTION_KEYS;

  static {
    java.util.Set<String> keys = new java.util.HashSet<>();
    keys.add(LanceSparkReadOptions.CONFIG_DATASET_URI);
    keys.add(LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS);
    keys.add(LanceSparkReadOptions.CONFIG_BLOCK_SIZE);
    keys.add(LanceSparkReadOptions.CONFIG_VERSION);
    keys.add(LanceSparkReadOptions.CONFIG_INDEX_CACHE_SIZE);
    keys.add(LanceSparkReadOptions.CONFIG_METADATA_CACHE_SIZE);
    keys.add(LanceSparkReadOptions.CONFIG_BATCH_SIZE);
    keys.add(LanceSparkReadOptions.CONFIG_BATCH_READAHEAD);
    keys.add(LanceSparkReadOptions.CONFIG_TOP_N_PUSH_DOWN);
    keys.add(LanceSparkReadOptions.CONFIG_NEAREST);
    keys.add(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH);
    keys.add(LanceSparkReadOptions.CONFIG_DATASET_CACHE_ENABLED);
    CONNECTOR_OWNED_OPTION_KEYS = Collections.unmodifiableSet(keys);
  }

  /**
   * Builds a stable cache key from URI, pinned version, catalog name, and a sorted snapshot of the
   * merged storage options. Callers MUST ensure {@code readOptions.getVersion() != null} before
   * invoking.
   *
   * <p>Public so {@code LanceScanBuilder} can share the exact same key recipe used by executor-side
   * {@link #create(int, LanceInputPartition)} — this lets the driver's single open seed the same
   * cache slot that executors will later checkout from on the same JVM.
   *
   * <p>Equality on the resulting key compares the full storage-options map by value, not just a
   * hash, so two queries with different credentials can never share a Dataset handle even if their
   * option maps hash-collide.
   *
   * <p>Connector-owned keys from {@link #CONNECTOR_OWNED_OPTION_KEYS} (e.g. {@code batch_size},
   * {@code dataset_cache_enabled}, {@code path}) are stripped before keying. These affect the
   * scanner / planner but not the underlying Dataset open, so two queries differing only on one of
   * these keys should share the same cache slot. Credential/region/endpoint keys remain.
   */
  public static LanceDatasetCache.CacheKey buildCacheKey(
      LanceSparkReadOptions readOptions, Map<String, String> initialStorageOptions) {
    Map<String, String> base =
        readOptions.getStorageOptions() != null
            ? readOptions.getStorageOptions()
            : Collections.emptyMap();
    Map<String, String> merged = LanceRuntime.mergeStorageOptions(base, initialStorageOptions);
    Map<String, String> filtered = new java.util.HashMap<>(merged.size());
    for (Map.Entry<String, String> entry : merged.entrySet()) {
      if (!CONNECTOR_OWNED_OPTION_KEYS.contains(entry.getKey())) {
        filtered.put(entry.getKey(), entry.getValue());
      }
    }
    return new LanceDatasetCache.CacheKey(
        readOptions.getDatasetUri(),
        readOptions.getVersion(),
        readOptions.getCatalogName(),
        filtered);
  }

  /**
   * @return the arrow reader. The caller is responsible for closing the reader
   */
  public ArrowReader getArrowReader() {
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    BufferAllocator allocator = LanceRuntime.allocator();

    ArrowReader reader;
    boolean useExecutorCache =
        LanceExecutorCache.isEnabled()
            && readOptions.isDatasetCacheEnabled()
            && readOptions.getVersion() != null;
    if (useExecutorCache) {
      LanceExecutorCacheKey execKey = buildExecutorCacheKey(readOptions);
      java.util.List<String> projectedCols = getColumnNames(inputPartition.getSchema());
      try {
        reader =
            LanceExecutorCache.getInstance()
                .getOrLoadColumns(
                    execKey, projectedCols, allocator, missCols -> scanner.scanBatches());
      } catch (IOException e) {
        throw LanceExceptions.wrap("load Lance fragment from executor disk cache", e);
      }
    } else {
      reader = scanner.scanBatches();
    }

    int queueDepth = readOptions.getBatchPrefetchQueueDepth();
    if (queueDepth <= 0) {
      return reader;
    }
    // PrefetchingArrowReader's ctor takes ownership of `reader` and closes it on any
    // post-ownership failure (schema read, child allocator / empty root creation, thread
    // start). We therefore must NOT also close `reader` here — Lance's native ArrowReader
    // is not idempotent under close(), and a second close on a JNI-backed reader can
    // SIGSEGV on double-free of native buffers.
    return new PrefetchingArrowReader(reader, queueDepth, allocator);
  }

  /**
   * Builds an {@link LanceExecutorCacheKey} identifying the decoded output of this fragment scan
   * under the requested projection/batchSize. Merged-auth storage options (minus connector-owned
   * keys) are hashed into the key so two queries with different credentials never share a cache
   * slot.
   *
   * <p>The WHERE clause and projected columns are <b>not</b> part of the key — this is a per-column
   * pre-filter cache. Each column is stored independently under the fragment directory, enabling
   * partial hits across queries with different projections. {@code LanceScanBuilder.pushFilters}
   * declines to push filters when the executor cache is enabled, so {@code scanner::scanBatches}
   * returns unfiltered rows and Spark's {@code Filter} operator applies the predicate post-cache.
   */
  private LanceExecutorCacheKey buildExecutorCacheKey(LanceSparkReadOptions readOptions) {
    java.util.Map<String, String> base =
        readOptions.getStorageOptions() != null
            ? readOptions.getStorageOptions()
            : java.util.Collections.emptyMap();
    java.util.Map<String, String> merged =
        LanceRuntime.mergeStorageOptions(base, inputPartition.getInitialStorageOptions());
    return new LanceExecutorCacheKey(
        readOptions.getDatasetUri(),
        readOptions.getVersion(),
        fragmentId,
        readOptions.getBatchSize(),
        merged);
  }

  @Override
  public void close() throws IOException {
    Throwable primary = null;
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Throwable t) {
        primary = t;
      }
    }
    // When the Dataset was obtained from the shared cache, release a ref-count instead of
    // closing it directly — the cache owns its lifecycle and evicts on TTL.
    if (dataset != null) {
      try {
        if (cacheKey != null) {
          LanceDatasetCache.release(cacheKey);
        } else {
          dataset.close();
        }
      } catch (Throwable t) {
        if (primary != null) {
          primary.addSuppressed(t);
        } else {
          primary = t;
        }
      }
    }
    if (primary != null) {
      if (primary instanceof IOException) {
        throw (IOException) primary;
      }
      if (primary instanceof RuntimeException) {
        throw (RuntimeException) primary;
      }
      if (primary instanceof Error) {
        throw (Error) primary;
      }
      throw new IOException(primary);
    }
  }

  public int fragmentId() {
    return fragmentId;
  }

  public boolean withFragemtId() {
    return withFragemtId;
  }

  public LanceInputPartition getInputPartition() {
    return inputPartition;
  }

  /**
   * Builds the projection column list for the scanner. Regular data columns come first, followed by
   * special metadata columns in the order matching {@link
   * org.lance.spark.LanceDataset#METADATA_COLUMNS}. All special columns (_rowid, _rowaddr, version
   * columns) go through scanner.project() for consistent output ordering.
   */
  private static List<String> getColumnNames(StructType schema) {
    // Collect all field names in the schema for quick lookup
    java.util.Set<String> schemaFields = new java.util.HashSet<>();
    for (StructField field : schema.fields()) {
      schemaFields.add(field.name());
    }

    // Regular data columns (exclude all special/metadata columns)
    List<String> columns =
        Arrays.stream(schema.fields())
            .map(StructField::name)
            .filter(
                name ->
                    !name.equals(LanceConstant.FRAGMENT_ID)
                        && !name.equals(LanceConstant.ROW_ID)
                        && !name.equals(LanceConstant.ROW_ADDRESS)
                        && !name.equals(LanceConstant.ROW_CREATED_AT_VERSION)
                        && !name.equals(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)
                        && !name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
                        && !name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX))
            .collect(Collectors.toList());

    // Append special columns in METADATA_COLUMNS order (must match Rust scanner output order)
    if (schemaFields.contains(LanceConstant.ROW_ID)) {
      columns.add(LanceConstant.ROW_ID);
    }
    if (schemaFields.contains(LanceConstant.ROW_ADDRESS)) {
      columns.add(LanceConstant.ROW_ADDRESS);
    }
    if (schemaFields.contains(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)) {
      columns.add(LanceConstant.ROW_LAST_UPDATED_AT_VERSION);
    }
    if (schemaFields.contains(LanceConstant.ROW_CREATED_AT_VERSION)) {
      columns.add(LanceConstant.ROW_CREATED_AT_VERSION);
    }

    return columns;
  }
}
