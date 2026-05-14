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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link LanceExecutorCache} with the per-column cache API.
 *
 * <p>The cache stores each column as a separate {@code .arrow} file under {@code
 * {cacheDir}/{fingerprint}/{colName}.arrow}. Tests verify hit/miss/partial-hit semantics, LRU
 * eviction at the fragment-directory level, stale tmp cleanup, and loader failure safety.
 *
 * <p>Uses a {@link StubArrowReader} that produces predictable int-valued batches for named columns.
 */
final class LanceExecutorCacheTest {

  /** Root allocator reset per-test to detect buffer leaks in both writer and reader paths. */
  private BufferAllocator allocator;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  void tearDown() {
    allocator.close();
  }

  @Test
  void missThenHit_sameColumns(@TempDir Path cacheDir) throws IOException {
    LanceExecutorCache cache = new LanceExecutorCache(cacheDir, 1L << 30);
    LanceExecutorCacheKey key = buildKey(1);
    List<String> columns = Arrays.asList("a", "b");
    // Each column gets 2 batches of 3 values
    Map<String, List<int[]>> columnData = new HashMap<>();
    columnData.put("a", Arrays.asList(new int[] {1, 2, 3}, new int[] {4, 5, 6}));
    columnData.put("b", Arrays.asList(new int[] {10, 20, 30}, new int[] {40, 50, 60}));
    AtomicInteger loaderCalls = new AtomicInteger();

    // First call: miss. Loader runs, data written to disk.
    try (ArrowReader first =
        cache.getOrLoadColumns(
            key,
            columns,
            allocator,
            missCols -> {
              loaderCalls.incrementAndGet();
              assertThat(missCols).containsExactlyElementsOf(columns);
              return new StubArrowReader(allocator, missCols, columnData);
            })) {
      Map<String, List<int[]>> got = drainAllColumns(first, columns);
      assertThat(got.get("a")).containsExactlyElementsOf(columnData.get("a"));
      assertThat(got.get("b")).containsExactlyElementsOf(columnData.get("b"));
    }
    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(cache.misses()).isEqualTo(1);
    assertThat(cache.hits()).isEqualTo(0);
    assertThat(cache.partialHits()).isEqualTo(0);

    // Second call: full hit. Loader should NOT be invoked.
    try (ArrowReader second =
        cache.getOrLoadColumns(
            key,
            columns,
            allocator,
            missCols -> {
              throw new AssertionError("loader must not be called on full hit");
            })) {
      Map<String, List<int[]>> got = drainAllColumns(second, columns);
      assertThat(got.get("a")).containsExactlyElementsOf(columnData.get("a"));
      assertThat(got.get("b")).containsExactlyElementsOf(columnData.get("b"));
    }
    assertThat(loaderCalls.get()).isEqualTo(1);
    assertThat(cache.hits()).isEqualTo(1);
    assertThat(cache.misses()).isEqualTo(1);
    assertThat(cache.partialHits()).isEqualTo(0);
  }

  @Test
  void partialHit_overlappingColumns(@TempDir Path cacheDir) throws IOException {
    LanceExecutorCache cache = new LanceExecutorCache(cacheDir, 1L << 30);
    LanceExecutorCacheKey key = buildKey(1);

    Map<String, List<int[]>> allData = new HashMap<>();
    allData.put("a", Collections.singletonList(new int[] {1, 2}));
    allData.put("b", Collections.singletonList(new int[] {3, 4}));
    allData.put("c", Collections.singletonList(new int[] {5, 6}));
    allData.put("d", Collections.singletonList(new int[] {7, 8}));

    // First call: request [a, b, c] — all miss
    List<String> firstCols = Arrays.asList("a", "b", "c");
    AtomicReference<List<String>> capturedMiss = new AtomicReference<>();
    try (ArrowReader r =
        cache.getOrLoadColumns(
            key,
            firstCols,
            allocator,
            missCols -> {
              capturedMiss.set(new ArrayList<>(missCols));
              return new StubArrowReader(allocator, missCols, allData);
            })) {
      drainAllColumns(r, firstCols);
    }
    assertThat(capturedMiss.get()).containsExactly("a", "b", "c");
    assertThat(cache.misses()).isEqualTo(1);

    // Second call: request [b, c, d] — b,c hit from cache, d is a miss
    List<String> secondCols = Arrays.asList("b", "c", "d");
    AtomicReference<List<String>> capturedMiss2 = new AtomicReference<>();
    try (ArrowReader r =
        cache.getOrLoadColumns(
            key,
            secondCols,
            allocator,
            missCols -> {
              capturedMiss2.set(new ArrayList<>(missCols));
              return new StubArrowReader(allocator, missCols, allData);
            })) {
      Map<String, List<int[]>> got = drainAllColumns(r, secondCols);
      assertThat(got.get("b")).containsExactlyElementsOf(allData.get("b"));
      assertThat(got.get("c")).containsExactlyElementsOf(allData.get("c"));
      assertThat(got.get("d")).containsExactlyElementsOf(allData.get("d"));
    }
    // Only column d was a miss
    assertThat(capturedMiss2.get()).containsExactly("d");
    assertThat(cache.partialHits()).isEqualTo(1);
  }

  @Test
  void differentFragments_independent(@TempDir Path cacheDir) throws IOException {
    LanceExecutorCache cache = new LanceExecutorCache(cacheDir, 1L << 30);
    LanceExecutorCacheKey k1 = buildKey(1);
    LanceExecutorCacheKey k2 = buildKey(2);
    List<String> columns = Collections.singletonList("x");

    Map<String, List<int[]>> data1 = new HashMap<>();
    data1.put("x", Collections.singletonList(new int[] {100, 200}));
    Map<String, List<int[]>> data2 = new HashMap<>();
    data2.put("x", Collections.singletonList(new int[] {900, 800}));

    // Load fragment 1
    try (ArrowReader r =
        cache.getOrLoadColumns(
            k1, columns, allocator, missCols -> new StubArrowReader(allocator, missCols, data1))) {
      Map<String, List<int[]>> got = drainAllColumns(r, columns);
      assertThat(got.get("x")).containsExactlyElementsOf(data1.get("x"));
    }
    // Load fragment 2
    try (ArrowReader r =
        cache.getOrLoadColumns(
            k2, columns, allocator, missCols -> new StubArrowReader(allocator, missCols, data2))) {
      Map<String, List<int[]>> got = drainAllColumns(r, columns);
      assertThat(got.get("x")).containsExactlyElementsOf(data2.get("x"));
    }
    assertThat(cache.entryCount()).isEqualTo(2);
    assertThat(cache.misses()).isEqualTo(2);

    // Hit fragment 1 — returns data1, not data2
    try (ArrowReader r =
        cache.getOrLoadColumns(
            k1,
            columns,
            allocator,
            missCols -> {
              throw new AssertionError("loader must not be called on hit");
            })) {
      Map<String, List<int[]>> got = drainAllColumns(r, columns);
      assertThat(got.get("x")).containsExactlyElementsOf(data1.get("x"));
    }
    assertThat(cache.hits()).isEqualTo(1);
  }

  @Test
  void lruEviction_deletesEntireDirectory(@TempDir Path cacheDir) throws IOException {
    // Limit = 1 byte so every write triggers eviction of older fragment directories.
    LanceExecutorCache cache = new LanceExecutorCache(cacheDir, 1L);
    List<String> columns = Collections.singletonList("v");

    // Write 3 fragment entries sequentially; each triggers eviction of older ones.
    for (int i = 1; i <= 3; i++) {
      LanceExecutorCacheKey k = buildKey(i);
      int val = i * 10;
      Map<String, List<int[]>> data = new HashMap<>();
      data.put("v", Collections.singletonList(new int[] {val, val + 1}));
      try (ArrowReader r =
          cache.getOrLoadColumns(
              k, columns, allocator, missCols -> new StubArrowReader(allocator, missCols, data))) {
        drainAllColumns(r, columns);
      }
    }

    // Only the last entry should survive under a 1-byte limit.
    assertThat(cache.entryCount()).isEqualTo(1);
    assertThat(cache.evictions()).isGreaterThanOrEqualTo(2);
    // Evicted entries' directories are deleted from disk
    try (java.util.stream.Stream<Path> dirs = Files.list(cacheDir)) {
      long dirCount = dirs.filter(Files::isDirectory).count();
      assertThat(dirCount).isEqualTo(1);
    }
  }

  @Test
  void staleTmpFile_deletedOnStartup(@TempDir Path cacheDir) throws IOException {
    // Create a stale .tmp file at the top level of cacheDir
    Files.createDirectories(cacheDir);
    Path staleTmp = cacheDir.resolve("deadbeef" + LanceExecutorCache.TMP_SUFFIX);
    Files.write(staleTmp, new byte[] {0x01, 0x02});

    // Also create a fragment directory with a stale .tmp inside
    Path fragDir = cacheDir.resolve("somefragdir");
    Files.createDirectories(fragDir);
    Path staleInFrag = fragDir.resolve("col_x" + LanceExecutorCache.TMP_SUFFIX);
    Files.write(staleInFrag, new byte[] {0x03, 0x04});

    assertThat(Files.exists(staleTmp)).isTrue();
    assertThat(Files.exists(staleInFrag)).isTrue();

    // Startup scan should delete stale tmp files
    new LanceExecutorCache(cacheDir, 1L << 30);

    assertThat(Files.exists(staleTmp)).isFalse();
    assertThat(Files.exists(staleInFrag)).isFalse();
  }

  @Test
  void loaderFailure_noPartialCommit(@TempDir Path cacheDir) throws IOException {
    LanceExecutorCache cache = new LanceExecutorCache(cacheDir, 1L << 30);
    LanceExecutorCacheKey key = buildKey(1);
    List<String> columns = Arrays.asList("a", "b");

    assertThatThrownBy(
            () ->
                cache.getOrLoadColumns(
                    key,
                    columns,
                    allocator,
                    missCols -> {
                      return new FailingArrowReader(
                          allocator, missCols, new IOException("synthetic loader failure"));
                    }))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("synthetic loader failure");

    // No .arrow files committed; tmp files cleaned up
    assertThat(cache.entryCount()).isEqualTo(0);
    try (java.util.stream.Stream<Path> files = Files.walk(cacheDir)) {
      long arrowFiles =
          files
              .filter(p -> p.getFileName().toString().endsWith(LanceExecutorCache.ARROW_SUFFIX))
              .count();
      assertThat(arrowFiles).isEqualTo(0);
    }
    try (java.util.stream.Stream<Path> files = Files.walk(cacheDir)) {
      long tmpFiles =
          files
              .filter(p -> p.getFileName().toString().endsWith(LanceExecutorCache.TMP_SUFFIX))
              .count();
      assertThat(tmpFiles).isEqualTo(0);
    }
  }

  @Test
  void logMetricsSummary_reflectsState(@TempDir Path cacheDir) throws IOException {
    LanceExecutorCache cache = new LanceExecutorCache(cacheDir, 1L << 30);
    // Log on empty cache — should not throw
    cache.logMetricsSummary("empty");
    assertThat(cache.hits()).isZero();
    assertThat(cache.misses()).isZero();
    assertThat(cache.partialHits()).isZero();

    // One miss, one hit, then verify metrics
    LanceExecutorCacheKey key = buildKey(1);
    List<String> columns = Collections.singletonList("v");
    Map<String, List<int[]>> data = new HashMap<>();
    data.put("v", Collections.singletonList(new int[] {1, 2, 3}));

    try (ArrowReader r =
        cache.getOrLoadColumns(
            key, columns, allocator, missCols -> new StubArrowReader(allocator, missCols, data))) {
      drainAllColumns(r, columns);
    }
    try (ArrowReader r =
        cache.getOrLoadColumns(
            key,
            columns,
            allocator,
            missCols -> {
              throw new AssertionError("hit path should not call loader");
            })) {
      drainAllColumns(r, columns);
    }
    assertThat(cache.hits()).isEqualTo(1);
    assertThat(cache.misses()).isEqualTo(1);
    assertThat(cache.partialHits()).isEqualTo(0);
    assertThat(cache.hitRate()).isGreaterThan(0.0);
    assertThat(cache.entryCount()).isEqualTo(1);

    // Logging itself must not mutate state
    cache.logMetricsSummary("after-ops");
    assertThat(cache.hits()).isEqualTo(1);
    assertThat(cache.misses()).isEqualTo(1);
  }

  // --- Helpers ---

  private static LanceExecutorCacheKey buildKey(int fragmentId) {
    return new LanceExecutorCacheKey(
        "s3://bucket/test.lance", 42, fragmentId, 4096, Collections.emptyMap());
  }

  /**
   * Drains all batches from a multi-column ArrowReader and returns a map of column name to list of
   * int[] batches.
   */
  private static Map<String, List<int[]>> drainAllColumns(ArrowReader reader, List<String> columns)
      throws IOException {
    Map<String, List<int[]>> result = new HashMap<>();
    for (String col : columns) {
      result.put(col, new ArrayList<>());
    }
    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    while (reader.loadNextBatch()) {
      for (String col : columns) {
        IntVector vec = (IntVector) root.getVector(col);
        int[] values = new int[vec.getValueCount()];
        for (int i = 0; i < values.length; i++) {
          values[i] = vec.get(i);
        }
        result.get(col).add(values);
      }
    }
    return result;
  }

  /**
   * Synthetic {@link ArrowReader} that yields predictable int-valued batches for multiple named
   * columns. The schema contains one {@code int32} field per column name. Each call to {@code
   * loadNextBatch()} advances through the batch list for all columns in lockstep.
   *
   * <p>The {@code columnData} map provides the batch data for each column. All columns must have
   * the same number of batches with the same row count per batch.
   */
  private static final class StubArrowReader extends ArrowReader {
    private final List<String> columns;
    private final Map<String, List<int[]>> columnData;
    private int idx = 0;
    private long bytesRead = 0;

    StubArrowReader(
        BufferAllocator allocator, List<String> columns, Map<String, List<int[]>> columnData) {
      super(allocator);
      this.columns = columns;
      this.columnData = columnData;
    }

    @Override
    protected Schema readSchema() {
      List<Field> fields =
          columns.stream()
              .map(
                  name -> new Field(name, FieldType.notNullable(new ArrowType.Int(32, true)), null))
              .collect(Collectors.toList());
      return new Schema(fields);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      ensureInitialized();
      // Use the first column's batch count as the reference
      List<int[]> firstColBatches = columnData.get(columns.get(0));
      if (firstColBatches == null || idx >= firstColBatches.size()) {
        return false;
      }
      VectorSchemaRoot root = getVectorSchemaRoot();
      int rowCount = firstColBatches.get(idx).length;
      for (String col : columns) {
        int[] values = columnData.get(col).get(idx);
        IntVector vec = (IntVector) root.getVector(col);
        vec.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
          vec.set(i, values[i]);
        }
        vec.setValueCount(values.length);
        bytesRead += values.length * 4L;
      }
      root.setRowCount(rowCount);
      idx++;
      return true;
    }

    @Override
    public long bytesRead() {
      return bytesRead;
    }

    @Override
    protected void closeReadSource() {
      // nothing to close
    }
  }

  /**
   * {@link ArrowReader} whose {@code loadNextBatch} throws — simulates loader failure. Has a valid
   * multi-column schema so the cache can set up writers before the failure.
   */
  private static final class FailingArrowReader extends ArrowReader {
    private final List<String> columns;
    private final IOException error;

    FailingArrowReader(BufferAllocator allocator, List<String> columns, IOException error) {
      super(allocator);
      this.columns = columns;
      this.error = error;
    }

    @Override
    protected Schema readSchema() {
      List<Field> fields =
          columns.stream()
              .map(
                  name -> new Field(name, FieldType.notNullable(new ArrowType.Int(32, true)), null))
              .collect(Collectors.toList());
      return new Schema(fields);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      throw error;
    }

    @Override
    public long bytesRead() {
      return 0L;
    }

    @Override
    protected void closeReadSource() {
      // nothing
    }
  }
}
