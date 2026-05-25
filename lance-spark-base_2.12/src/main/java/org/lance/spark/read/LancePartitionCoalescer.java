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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Bin-packs single-fragment {@link LanceSplit}s into multi-fragment splits using the same algorithm
 * Spark applies to file-source partitioning (see {@code FilePartition.getFilePartitions} in Spark
 * SQL).
 *
 * <p>Without this step, the V2 datasource emits one Spark partition per Lance fragment. On large
 * datasets that creates hundreds of tasks where most do trivial decode work — the per-task launch /
 * IPC overhead dominates. Coalescing surviving fragments into ~{@code maxBytesPerPartition}
 * groupings brings task counts in line with Spark's parquet/orc behaviour.
 *
 * <p>Algorithm (mirrors {@code FilePartition.getFilePartitions}):
 *
 * <ol>
 *   <li>Walk fragments in input order (preserving any upstream pruning sequence).
 *   <li>Open a fresh partition when {@code currentBytes + fragmentBytes > maxBytesPerPartition}.
 *   <li>Accumulate {@code currentBytes += fragmentBytes + openCostBytes} so a partition packed with
 *       many tiny fragments still incurs a virtual per-fragment overhead.
 * </ol>
 *
 * <p>If the byte size for any input fragment is missing or {@code null}, this returns the input
 * unchanged (1:1 fallback). Coalescing without all sizes risks unbalanced or oversized partitions,
 * which is worse than the status quo.
 */
public final class LancePartitionCoalescer {

  private LancePartitionCoalescer() {}

  /**
   * Coalesces single-fragment splits into byte-bounded multi-fragment splits.
   *
   * @param singleFragmentSplits each entry must contain exactly one fragment ID; multi-fragment
   *     inputs are returned unchanged so coalescing never overrides upstream grouping.
   * @param fragmentByteSizes maps fragment ID → bytes; absent or {@code null} entries trigger a 1:1
   *     fallback.
   * @param maxBytesPerPartition target upper bound; values {@code <= 0} disable coalescing.
   * @param openCostBytes virtual per-fragment overhead, mirroring {@code
   *     spark.sql.files.openCostInBytes}.
   * @return a list of {@link LanceSplit} grouped by target bytes (input order preserved).
   */
  public static List<LanceSplit> coalesce(
      List<LanceSplit> singleFragmentSplits,
      Map<Integer, Long> fragmentByteSizes,
      long maxBytesPerPartition,
      long openCostBytes) {
    if (maxBytesPerPartition <= 0 || singleFragmentSplits.isEmpty()) {
      return singleFragmentSplits;
    }
    long openCost = Math.max(openCostBytes, 0L);

    List<long[]> entries = new ArrayList<>(singleFragmentSplits.size());
    for (LanceSplit split : singleFragmentSplits) {
      List<Integer> fragments = split.getFragments();
      if (fragments.size() != 1) {
        // Caller already grouped fragments — leave as-is to avoid disturbing upstream pruners.
        return singleFragmentSplits;
      }
      int fragmentId = fragments.get(0);
      Long size = fragmentByteSizes.get(fragmentId);
      if (size == null || size < 0) {
        // Without a reliable size we cannot bin-pack safely.
        return singleFragmentSplits;
      }
      entries.add(new long[] {fragmentId, size});
    }

    List<LanceSplit> coalesced = new ArrayList<>();
    List<Integer> currentFragments = new ArrayList<>();
    long currentBytes = 0L;
    for (long[] entry : entries) {
      int fragmentId = (int) entry[0];
      long fragmentBytes = entry[1];
      if (!currentFragments.isEmpty() && currentBytes + fragmentBytes > maxBytesPerPartition) {
        coalesced.add(new LanceSplit(Collections.unmodifiableList(currentFragments)));
        currentFragments = new ArrayList<>();
        currentBytes = 0L;
      }
      currentFragments.add(fragmentId);
      currentBytes += fragmentBytes + openCost;
    }
    if (!currentFragments.isEmpty()) {
      coalesced.add(new LanceSplit(Collections.unmodifiableList(currentFragments)));
    }
    return coalesced;
  }
}
