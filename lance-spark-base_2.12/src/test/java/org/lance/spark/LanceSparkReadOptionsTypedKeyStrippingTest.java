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
package org.lance.spark;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that recognized typed read options (e.g. {@code batch_prefetch_queue_depth}, {@code
 * batch_readahead}, {@code block_size}, {@code path}) are stripped from {@link
 * LanceSparkReadOptions#getStorageOptions()} after parsing.
 *
 * <p>Motivation: {@code storageOptions} is forwarded verbatim to the native Lance storage layer (S3
 * / OSS / GCS credential + endpoint map). Typed connector-level knobs were previously leaking into
 * that map because {@code Builder.fromOptions} saved the entire input map as {@code storageOptions}
 * before {@code parseTypedFlags} promoted recognized keys to their dedicated builder fields. The
 * Rust layer silently drops unknown keys, so no functional breakage, but it muddies the native log
 * and complicates debugging of credential / endpoint issues.
 *
 * <p>Pure connector-level knobs and the dataset path must not appear in the storage-options map at
 * all. Keys the native layer consumes directly (e.g. {@code aws_region}, {@code aws_endpoint}) must
 * still pass through untouched.
 */
public class LanceSparkReadOptionsTypedKeyStrippingTest {

  @Test
  public void batchPrefetchQueueDepthIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "8");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);

    assertEquals(8, options.getBatchPrefetchQueueDepth(), "typed field must be populated");
    assertFalse(
        options
            .getStorageOptions()
            .containsKey(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH),
        "batch_prefetch_queue_depth must not leak into Rust storage_options");
  }

  @Test
  public void batchReadaheadIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkReadOptions.CONFIG_BATCH_READAHEAD, "32");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);

    assertEquals(32, options.getBatchReadahead());
    assertFalse(
        options.getStorageOptions().containsKey(LanceSparkReadOptions.CONFIG_BATCH_READAHEAD),
        "batch_readahead must not leak into Rust storage_options");
  }

  @Test
  public void blockSizeIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkReadOptions.CONFIG_BLOCK_SIZE, "1048576");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);

    assertEquals(Integer.valueOf(1048576), options.getBlockSize());
    assertFalse(
        options.getStorageOptions().containsKey(LanceSparkReadOptions.CONFIG_BLOCK_SIZE),
        "block_size must not leak into Rust storage_options");
  }

  @Test
  public void batchSizeIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkReadOptions.CONFIG_BATCH_SIZE, "4096");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);

    assertEquals(4096, options.getBatchSize());
    assertFalse(
        options.getStorageOptions().containsKey(LanceSparkReadOptions.CONFIG_BATCH_SIZE),
        "batch_size must not leak into Rust storage_options");
  }

  @Test
  public void datasetUriPathKeyIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);

    assertFalse(
        options.getStorageOptions().containsKey(LanceSparkReadOptions.CONFIG_DATASET_URI),
        "path must not leak into Rust storage_options — it is a connector-level URL, not a "
            + "storage credential");
  }

  @Test
  public void pushDownFiltersIsStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS, "false");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);

    assertFalse(options.isPushDownFilters());
    assertFalse(
        options.getStorageOptions().containsKey(LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS));
  }

  @Test
  public void versionAndCacheSizesAreStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkReadOptions.CONFIG_VERSION, "42");
    opts.put(LanceSparkReadOptions.CONFIG_INDEX_CACHE_SIZE, "1024");
    opts.put(LanceSparkReadOptions.CONFIG_METADATA_CACHE_SIZE, "2048");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);

    assertEquals(Integer.valueOf(42), options.getVersion());
    assertEquals(Integer.valueOf(1024), options.getIndexCacheSize());
    assertEquals(Integer.valueOf(2048), options.getMetadataCacheSize());

    Map<String, String> storage = options.getStorageOptions();
    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_VERSION));
    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_INDEX_CACHE_SIZE));
    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_METADATA_CACHE_SIZE));
  }

  @Test
  public void executorCredentialRefreshAndDatasetCacheKeysAreStripped() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH, "false");
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_CACHE_ENABLED, "false");
    opts.put(LanceSparkReadOptions.CONFIG_TOP_N_PUSH_DOWN, "false");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);

    assertFalse(options.isExecutorCredentialRefresh());
    assertFalse(options.isDatasetCacheEnabled());
    assertFalse(options.isTopNPushDown());

    Map<String, String> storage = options.getStorageOptions();
    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH));
    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_DATASET_CACHE_ENABLED));
    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_TOP_N_PUSH_DOWN));
  }

  @Test
  public void genuineStorageKeysPassThroughUntouched() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    // Typed connector key (must be stripped)
    opts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "4");
    // Genuine Rust-side storage options (must pass through)
    opts.put("aws_region", "us-east-1");
    opts.put("aws_endpoint", "http://localhost:9000");
    opts.put("aws_access_key_id", "minioadmin");
    opts.put("aws_secret_access_key", "minioadmin");
    opts.put("allow_http", "true");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);
    Map<String, String> storage = options.getStorageOptions();

    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH));
    assertEquals("us-east-1", storage.get("aws_region"));
    assertEquals("http://localhost:9000", storage.get("aws_endpoint"));
    assertEquals("minioadmin", storage.get("aws_access_key_id"));
    assertEquals("minioadmin", storage.get("aws_secret_access_key"));
    assertEquals("true", storage.get("allow_http"));
  }

  @Test
  public void catalogDefaultsWithTypedKeysDoNotLeak() {
    // Simulates spark-defaults.conf: spark.sql.catalog.foo.batch_prefetch_queue_depth=8
    //                                 spark.sql.catalog.foo.aws_region=us-east-1
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "8");
    catalogOpts.put(LanceSparkReadOptions.CONFIG_BATCH_READAHEAD, "32");
    catalogOpts.put("aws_region", "us-east-1");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .withCatalogDefaults(catalogConfig)
            .build();

    assertEquals(8, options.getBatchPrefetchQueueDepth());
    assertEquals(32, options.getBatchReadahead());

    Map<String, String> storage = options.getStorageOptions();
    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH));
    assertFalse(storage.containsKey(LanceSparkReadOptions.CONFIG_BATCH_READAHEAD));
    assertEquals("us-east-1", storage.get("aws_region"));
  }

  @Test
  public void perReadOptionOverridesCatalogAndDoesNotLeak() {
    Map<String, String> catalogOpts = new HashMap<>();
    catalogOpts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "8");
    LanceSparkCatalogConfig catalogConfig = LanceSparkCatalogConfig.from(catalogOpts);

    Map<String, String> readOpts = new HashMap<>();
    readOpts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    readOpts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "16");

    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .fromOptions(readOpts)
            .withCatalogDefaults(catalogConfig)
            .build();

    // Per-read .option() must win over catalog default.
    assertEquals(16, options.getBatchPrefetchQueueDepth());
    assertFalse(
        options
            .getStorageOptions()
            .containsKey(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH),
        "catalog + per-read merge must still strip typed keys");
  }

  @Test
  public void noTypedKeysStillLeavesGenuineStorageOptionsIntact() {
    // No typed read options set — only storage credentials. Ensures the strip
    // operation is a no-op when there's nothing to strip.
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    opts.put("aws_region", "us-east-1");
    opts.put("aws_endpoint", "http://localhost:9000");

    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);
    Map<String, String> storage = options.getStorageOptions();

    assertTrue(storage.size() >= 2, "genuine storage entries must survive");
    assertEquals("us-east-1", storage.get("aws_region"));
    assertEquals("http://localhost:9000", storage.get("aws_endpoint"));
  }
}
