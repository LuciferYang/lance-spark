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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LancePartitionCoalescer}, the byte-bin-packing helper that mimics Spark's
 * {@code spark.sql.files.maxPartitionBytes} behaviour for Lance fragments.
 */
public class LancePartitionCoalescerTest {

  private static final long MB = 1024L * 1024L;

  /** Convenience: build a list of one-fragment splits in fragment-id order. */
  private static List<LanceSplit> singletonSplits(int... fragmentIds) {
    return Arrays.stream(fragmentIds)
        .mapToObj(id -> new LanceSplit(Collections.singletonList(id)))
        .collect(Collectors.toList());
  }

  /** Convenience: per-fragment byte size map. */
  private static Map<Integer, Long> sizes(Object... pairs) {
    if (pairs.length % 2 != 0) {
      throw new IllegalArgumentException("pairs must come in (id, size) tuples");
    }
    Map<Integer, Long> m = new HashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      m.put((Integer) pairs[i], (Long) pairs[i + 1]);
    }
    return m;
  }

  @Test
  public void allSmallFragmentsPackIntoOnePartition() {
    // 4 fragments × 10MB, target 128MB → all fit in one partition.
    List<LanceSplit> input = singletonSplits(0, 1, 2, 3);
    Map<Integer, Long> byteSizes = sizes(0, 10 * MB, 1, 10 * MB, 2, 10 * MB, 3, 10 * MB);

    List<LanceSplit> output = LancePartitionCoalescer.coalesce(input, byteSizes, 128 * MB, 4 * MB);

    assertEquals(1, output.size());
    assertEquals(Arrays.asList(0, 1, 2, 3), output.get(0).getFragments());
  }

  @Test
  public void packsBoundaryAtTargetBytes() {
    // 30 × 10MB = 300MB, target 128MB, openCost 4MB → each fragment costs 14MB virtually.
    // 128 / 14 = 9 fragments per partition (126MB virtual). 30 fragments → ceil(30/9) = 4
    // partitions.
    List<LanceSplit> input = singletonSplits(java.util.stream.IntStream.range(0, 30).toArray());
    Map<Integer, Long> byteSizes = new HashMap<>();
    for (int i = 0; i < 30; i++) {
      byteSizes.put(i, 10 * MB);
    }

    List<LanceSplit> output = LancePartitionCoalescer.coalesce(input, byteSizes, 128 * MB, 4 * MB);

    // Each partition holds at most floor(128 / 14) = 9 fragments by the greedy bin-packer.
    int totalFragments = output.stream().mapToInt(s -> s.getFragments().size()).sum();
    assertEquals(30, totalFragments);
    for (LanceSplit split : output) {
      assertTrue(
          split.getFragments().size() <= 9,
          "partition holds " + split.getFragments().size() + " fragments, expected <= 9");
    }
    // Tighter shape check: 4 partitions of size {9, 9, 9, 3}.
    assertEquals(4, output.size());
  }

  @Test
  public void singleOversizedFragmentBecomesSoloSplit() {
    // 200MB single fragment > target 128MB → kept solo (we never split a single fragment).
    List<LanceSplit> input = singletonSplits(0, 1, 2);
    Map<Integer, Long> byteSizes = sizes(0, 50 * MB, 1, 200 * MB, 2, 50 * MB);

    List<LanceSplit> output = LancePartitionCoalescer.coalesce(input, byteSizes, 128 * MB, 4 * MB);

    // frag 0 → partition A (50MB)
    // frag 1 (200MB) breaks A, becomes solo partition B
    // frag 2 → partition C (50MB)
    assertEquals(3, output.size());
    assertEquals(Collections.singletonList(0), output.get(0).getFragments());
    assertEquals(Collections.singletonList(1), output.get(1).getFragments());
    assertEquals(Collections.singletonList(2), output.get(2).getFragments());
  }

  @Test
  public void preservesFragmentIdOrder() {
    // Ensure scan-order stability: bin-packing must not reorder fragments.
    List<LanceSplit> input = singletonSplits(7, 3, 11, 1);
    Map<Integer, Long> byteSizes = sizes(7, 30 * MB, 3, 30 * MB, 11, 30 * MB, 1, 30 * MB);

    List<LanceSplit> output = LancePartitionCoalescer.coalesce(input, byteSizes, 128 * MB, 4 * MB);

    // All 4 × 34MB virtual = 136MB; target 128MB → 7,3,11 fit (102MB), 1 spills.
    assertEquals(2, output.size());
    assertEquals(Arrays.asList(7, 3, 11), output.get(0).getFragments());
    assertEquals(Collections.singletonList(1), output.get(1).getFragments());
  }

  @Test
  public void targetZeroOrNegativeReturnsInputUnchanged() {
    List<LanceSplit> input = singletonSplits(0, 1, 2);
    Map<Integer, Long> byteSizes = sizes(0, 10 * MB, 1, 10 * MB, 2, 10 * MB);

    assertSame(input, LancePartitionCoalescer.coalesce(input, byteSizes, 0, 4 * MB));
    assertSame(input, LancePartitionCoalescer.coalesce(input, byteSizes, -1, 4 * MB));
  }

  @Test
  public void anyMissingFragmentSizeFallsBackToOneToOne() {
    // If we cannot estimate any fragment's size, refuse to coalesce — return the input unchanged.
    List<LanceSplit> input = singletonSplits(0, 1, 2);
    Map<Integer, Long> byteSizes = sizes(0, 10 * MB, 2, 10 * MB); // fragment 1 missing

    List<LanceSplit> output = LancePartitionCoalescer.coalesce(input, byteSizes, 128 * MB, 4 * MB);
    assertSame(input, output);
  }

  @Test
  public void nullFragmentSizeFallsBackToOneToOne() {
    // An explicit null entry signals "size unknown" the same way as a missing entry.
    List<LanceSplit> input = singletonSplits(0, 1, 2);
    Map<Integer, Long> byteSizes = new HashMap<>();
    byteSizes.put(0, 10 * MB);
    byteSizes.put(1, null);
    byteSizes.put(2, 10 * MB);

    List<LanceSplit> output = LancePartitionCoalescer.coalesce(input, byteSizes, 128 * MB, 4 * MB);
    assertSame(input, output);
  }

  @Test
  public void inputWithMultiFragmentSplitsLeftAlone() {
    // Coalesce only operates on single-fragment splits (the planScan invariant). If the caller has
    // already produced multi-fragment splits, return them unchanged so we never destabilize an
    // upstream pruner that pre-grouped fragments.
    List<LanceSplit> alreadyGrouped =
        Collections.singletonList(new LanceSplit(Arrays.asList(0, 1, 2)));
    Map<Integer, Long> byteSizes = sizes(0, 10 * MB, 1, 10 * MB, 2, 10 * MB);

    List<LanceSplit> output =
        LancePartitionCoalescer.coalesce(alreadyGrouped, byteSizes, 128 * MB, 4 * MB);
    assertSame(alreadyGrouped, output);
  }

  @Test
  public void emptyInputReturnsEmptyList() {
    List<LanceSplit> output =
        LancePartitionCoalescer.coalesce(
            Collections.emptyList(), new HashMap<>(), 128 * MB, 4 * MB);
    assertTrue(output.isEmpty());
  }
}
