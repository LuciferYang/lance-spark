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
import org.lance.WriteParams;
import org.lance.spark.LanceRuntime;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * Benchmark comparing eager fragment loading ({@code dataset.getFragments()}) vs lazy on-demand
 * loading ({@code dataset.getFragment(id)}).
 *
 * <p>This benchmark supports the performance claim in PR #353: removing eager fragment pre-loading
 * reduces per-partition initialization cost from O(N) to O(1) where N is the total fragment count.
 *
 * <p>Tagged with "benchmark" so it is excluded from normal test runs.
 */
@Tag("benchmark")
public class FragmentLoadingBenchmarkTest {

  @TempDir Path tempDir;

  private static final int WARMUP_ITERATIONS = 3;
  private static final int MEASURE_ITERATIONS = 10;
  private static final int ROWS_PER_FRAGMENT = 100;

  @Test
  public void benchmarkFragmentLoadingStrategies() throws Exception {
    int[] fragmentCounts = {10, 50, 100, 500, 1000};

    System.out.println();
    System.out.println("=== Fragment Loading Benchmark ===");
    System.out.println(
        String.format(
            "%-12s | %20s | %20s | %10s",
            "Fragments", "getFragments() (ms)", "getFragment(id) (ms)", "Speedup"));
    System.out.println("-".repeat(70));

    for (int numFragments : fragmentCounts) {
      String datasetUri = createDatasetWithFragments(numFragments);
      double eagerTimeMs = benchmarkEagerLoading(datasetUri, numFragments);
      double lazyTimeMs = benchmarkLazyLoading(datasetUri, numFragments);
      double speedup = eagerTimeMs / lazyTimeMs;

      System.out.println(
          String.format(
              "%-12d | %17.3f ms | %17.3f ms | %9.1fx",
              numFragments, eagerTimeMs, lazyTimeMs, speedup));
    }

    System.out.println();
  }

  /**
   * Creates a dataset with the specified number of fragments. Each append creates one new fragment.
   */
  private String createDatasetWithFragments(int numFragments) throws Exception {
    String datasetUri = tempDir.resolve("bench_" + numFragments + ".lance").toString();
    BufferAllocator allocator = LanceRuntime.allocator();

    Field field = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Collections.singletonList(field));

    // Create first fragment, then append the rest
    for (int f = 0; f < numFragments; f++) {
      WriteParams.WriteMode mode =
          f == 0 ? WriteParams.WriteMode.CREATE : WriteParams.WriteMode.APPEND;
      appendFragment(allocator, datasetUri, schema, f, mode);
    }

    // Verify fragment count
    try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
      List<Fragment> fragments = dataset.getFragments();
      if (fragments.size() != numFragments) {
        throw new IllegalStateException(
            "Expected " + numFragments + " fragments but got " + fragments.size());
      }
    }

    return datasetUri;
  }

  private void appendFragment(
      BufferAllocator allocator,
      String datasetUri,
      Schema schema,
      int fragmentIndex,
      WriteParams.WriteMode mode)
      throws Exception {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      for (int i = 0; i < ROWS_PER_FRAGMENT; i++) {
        idVec.setSafe(i, fragmentIndex * ROWS_PER_FRAGMENT + i);
      }
      root.setRowCount(ROWS_PER_FRAGMENT);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      try (ArrowStreamReader reader =
              new ArrowStreamReader(new ByteArrayInputStream(baos.toByteArray()), allocator);
          ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, reader, arrowStream);
        Dataset.write().stream(arrowStream).uri(datasetUri).mode(mode).execute().close();
      }
    }
  }

  /**
   * Benchmarks the OLD approach: call getFragments() which loads ALL fragment metadata, then pick
   * one fragment from the returned list. This is what the old LanceDatasetCache did on every cache
   * miss.
   */
  private double benchmarkEagerLoading(String datasetUri, int numFragments) {
    BufferAllocator allocator = LanceRuntime.allocator();
    int targetFragmentId = numFragments / 2; // pick a fragment in the middle

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        List<Fragment> allFragments = dataset.getFragments();
        // Simulate old code: find target fragment from the full list
        allFragments.stream().filter(f -> f.getId() == targetFragmentId).findFirst().orElseThrow();
      }
    }

    // Measure
    long totalNanos = 0;
    for (int i = 0; i < MEASURE_ITERATIONS; i++) {
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        long start = System.nanoTime();
        List<Fragment> allFragments = dataset.getFragments();
        allFragments.stream().filter(f -> f.getId() == targetFragmentId).findFirst().orElseThrow();
        totalNanos += System.nanoTime() - start;
      }
    }

    return (totalNanos / (double) MEASURE_ITERATIONS) / 1_000_000.0;
  }

  /**
   * Benchmarks the NEW approach: call getFragment(id) which loads ONLY the requested fragment
   * metadata. This is what the current LanceRuntime.getFragment() does.
   */
  private double benchmarkLazyLoading(String datasetUri, int numFragments) {
    BufferAllocator allocator = LanceRuntime.allocator();
    int targetFragmentId = numFragments / 2;

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        dataset.getFragment(targetFragmentId);
      }
    }

    // Measure
    long totalNanos = 0;
    for (int i = 0; i < MEASURE_ITERATIONS; i++) {
      try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
        long start = System.nanoTime();
        dataset.getFragment(targetFragmentId);
        totalNanos += System.nanoTime() - start;
      }
    }

    return (totalNanos / (double) MEASURE_ITERATIONS) / 1_000_000.0;
  }
}
