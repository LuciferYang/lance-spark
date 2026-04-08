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
package org.lance.spark.utils;

import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.Version;
import org.lance.namespace.LanceNamespace;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkCatalogConfig;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Utils {

  public static long parseVersion(String version) {
    return Long.parseUnsignedLong(version);
  }

  public static long findVersion(List<Version> versions, long timestamp) {
    long versionID = -1;
    Instant instant = instantFromTimestamp(timestamp);
    for (Version version : versions) {
      // Truncate version timestamp to microsecond precision to match Spark's
      // microsecond timestamp resolution, avoiding sub-microsecond mismatches
      Instant versionInstant = truncateToMicros(version.getDataTime().toInstant());
      if (versionInstant.compareTo(instant) <= 0) {
        versionID = version.getId();
      } else {
        break;
      }
    }
    if (versionID == -1) {
      throw new IllegalArgumentException("No version found with timestamp: " + timestamp);
    }
    return versionID;
  }

  /** Opens a dataset via namespace path with read options (storage credentials, etc.). */
  private static Dataset openDataset(
      LanceNamespace namespace, List<String> tableId, ReadOptions readOptions) {
    return Dataset.open()
        .allocator(LanceRuntime.allocator())
        .namespaceClient(namespace)
        .tableId(tableId)
        .readOptions(readOptions)
        .build();
  }

  /** Opens a dataset via URI with the given read options. */
  public static Dataset openDataset(String uri, ReadOptions readOptions) {
    return Dataset.open()
        .allocator(LanceRuntime.allocator())
        .uri(uri)
        .readOptions(readOptions)
        .build();
  }

  /** Opens a dataset using read options, dispatching to namespace or URI path. */
  public static Dataset openDataset(LanceSparkReadOptions readOptions) {
    if (readOptions.hasNamespace()) {
      return openDataset(
          readOptions.getNamespace(), readOptions.getTableId(), readOptions.toReadOptions());
    }
    return openDataset(readOptions.getDatasetUri(), readOptions.toReadOptions());
  }

  /** Opens a dataset using write options, dispatching to namespace or URI path. */
  public static Dataset openDataset(LanceSparkWriteOptions writeOptions) {
    if (writeOptions.hasNamespace()) {
      return openDataset(
          writeOptions.getNamespace(), writeOptions.getTableId(), writeOptions.toReadOptions());
    }
    return openDataset(writeOptions.getDatasetUri(), writeOptions.toReadOptions());
  }

  /** Returns a new builder for opens that need storage-option merging or a specific session. */
  public static OpenDatasetBuilder openDatasetBuilder() {
    return new OpenDatasetBuilder();
  }

  /**
   * Builder for dataset opens that merge driver-side {@code initialStorageOptions} into the base
   * storage options and attach a managed {@link LanceRuntime} session.
   *
   * <p>Prefer {@link #readOptions(LanceSparkReadOptions)} or {@link
   * #writeOptions(LanceSparkWriteOptions)} to populate fields from existing option objects; use the
   * individual setters only when those are not available.
   */
  public static class OpenDatasetBuilder {
    private String uri;
    private Map<String, String> storageOptions;
    private Map<String, String> initialStorageOptions;
    private String catalogName;
    private Long version;

    private OpenDatasetBuilder() {}

    public OpenDatasetBuilder uri(String uri) {
      this.uri = uri;
      return this;
    }

    public OpenDatasetBuilder storageOptions(Map<String, String> storageOptions) {
      this.storageOptions = storageOptions;
      return this;
    }

    /** Populates URI, storage options, version, and catalog name from the read options. */
    public OpenDatasetBuilder readOptions(LanceSparkReadOptions readOptions) {
      this.uri = readOptions.getDatasetUri();
      this.storageOptions = readOptions.getStorageOptions();
      this.version = readOptions.getVersion() != null ? readOptions.getVersion().longValue() : null;
      this.catalogName = readOptions.getCatalogName();
      return this;
    }

    /** Populates URI and storage options from the write options. */
    public OpenDatasetBuilder writeOptions(LanceSparkWriteOptions writeOptions) {
      this.uri = writeOptions.getDatasetUri();
      this.storageOptions = writeOptions.getStorageOptions();
      return this;
    }

    /** Sets initial storage options obtained from {@code describeTable()} on the driver. */
    public OpenDatasetBuilder initialStorageOptions(Map<String, String> initialStorageOptions) {
      this.initialStorageOptions = initialStorageOptions;
      return this;
    }

    /** Sets the catalog name used to pick an isolated {@link LanceRuntime} session. */
    public OpenDatasetBuilder catalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    public OpenDatasetBuilder version(Long version) {
      this.version = version;
      return this;
    }

    public Dataset build() {
      Map<String, String> base = storageOptions != null ? storageOptions : Collections.emptyMap();
      Map<String, String> merged = LanceRuntime.mergeStorageOptions(base, initialStorageOptions);

      ReadOptions.Builder roBuilder =
          new ReadOptions.Builder()
              .setStorageOptions(merged)
              .setSession(
                  catalogName != null ? LanceRuntime.session(catalogName) : LanceRuntime.session());
      if (version != null) {
        roBuilder.setVersion(version);
      }

      return Dataset.open()
          .allocator(LanceRuntime.allocator())
          .uri(uri)
          .readOptions(roBuilder.build())
          .build();
    }
  }

  /**
   * Creates LanceSparkReadOptions for this catalog.
   *
   * @param location the dataset URI
   * @param catalogConfig catalog configuration
   * @param versionId optional dataset version id
   * @param namespace optional namespace for credential vending
   * @param tableId optional table identifier
   * @param catalogName catalog name for cache isolation
   * @return a new LanceSparkReadOptions with catalog settings
   */
  public static LanceSparkReadOptions createReadOptions(
      String location,
      LanceSparkCatalogConfig catalogConfig,
      Optional<Long> versionId,
      Optional<LanceNamespace> namespace,
      Optional<List<String>> tableId,
      String catalogName) {
    LanceSparkReadOptions.Builder builder =
        LanceSparkReadOptions.builder()
            .datasetUri(location)
            .withCatalogDefaults(catalogConfig)
            .catalogName(catalogName);

    if (versionId.isPresent()) {
      builder.version(versionId.get().intValue());
    }
    if (tableId.isPresent()) {
      builder.tableId(tableId.get());
    }
    if (namespace.isPresent()) {
      builder.namespace(namespace.get());
    }

    return builder.build();
  }

  // Determine if the timestamp is in microseconds or nanoseconds and convert to Instant
  private static Instant instantFromTimestamp(long timestamp) {
    if (timestamp <= 0) {
      throw new IllegalArgumentException("Timestamp must be greater than zero");
    }
    return instantFromEpochMicros(timestamp);
  }

  private static Instant instantFromEpochMicros(long epochMicros) {
    long sec = Math.floorDiv(epochMicros, 1_000_000L);
    long nanoAdj = Math.floorMod(epochMicros, 1_000_000L) * 1_000L;
    return Instant.ofEpochSecond(sec, nanoAdj);
  }

  private static Instant truncateToMicros(Instant instant) {
    long nanos = instant.getNano();
    long truncatedNanos = (nanos / 1_000L) * 1_000L;
    return Instant.ofEpochSecond(instant.getEpochSecond(), truncatedNanos);
  }
}
