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
import org.lance.fragment.DataFile;
import org.lance.spark.LanceSparkReadOptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceSplit implements Serializable {
  private static final long serialVersionUID = 2983749283749283749L;

  private final List<Integer> fragments;

  public LanceSplit(List<Integer> fragments) {
    this.fragments = fragments;
  }

  public List<Integer> getFragments() {
    return fragments;
  }

  /**
   * Result of scan planning containing splits, resolved version, per-fragment row counts, and
   * per-fragment byte sizes (for partition coalescing downstream).
   */
  public static class ScanPlanResult {
    private final List<LanceSplit> splits;
    private final long resolvedVersion;

    /** Per-fragment logical row counts (after deletions). Key is fragment ID. */
    private final Map<Integer, Long> fragmentRowCounts;

    /**
     * Per-fragment byte size summed across all data files. Key is fragment ID; values may be {@code
     * null} when {@link DataFile#getFileSizeBytes()} is unavailable for any contributing file (e.g.
     * older datasets written before file-size tracking landed). Downstream coalescers must treat
     * {@code null} as unknown and fall back to one-fragment-per-partition.
     */
    private final Map<Integer, Long> fragmentByteSizes;

    public ScanPlanResult(
        List<LanceSplit> splits, long resolvedVersion, Map<Integer, Long> fragmentRowCounts) {
      this(splits, resolvedVersion, fragmentRowCounts, Collections.emptyMap());
    }

    public ScanPlanResult(
        List<LanceSplit> splits,
        long resolvedVersion,
        Map<Integer, Long> fragmentRowCounts,
        Map<Integer, Long> fragmentByteSizes) {
      this.splits = splits;
      this.resolvedVersion = resolvedVersion;
      this.fragmentRowCounts = fragmentRowCounts;
      this.fragmentByteSizes = fragmentByteSizes;
    }

    public List<LanceSplit> getSplits() {
      return splits;
    }

    public long getResolvedVersion() {
      return resolvedVersion;
    }

    public Map<Integer, Long> getFragmentRowCounts() {
      return fragmentRowCounts;
    }

    public Map<Integer, Long> getFragmentByteSizes() {
      return fragmentByteSizes;
    }
  }

  /**
   * Generates splits and resolves the dataset version.
   *
   * <p>This method opens the dataset at the specified version (or latest if not specified), gets
   * the fragment IDs and per-fragment row counts, and returns both the splits and the resolved
   * version. The resolved version should be passed to workers to ensure snapshot isolation.
   */
  public static ScanPlanResult planScan(LanceSparkReadOptions readOptions) {
    org.lance.spark.internal.LanceDatasetCache.OpenResult open =
        org.lance.spark.internal.LanceDatasetCache.getOrOpen(readOptions);
    Dataset dataset = open.dataset();
    try {
      List<Fragment> fragments = dataset.getFragments();
      List<LanceSplit> splits = new ArrayList<>(fragments.size());
      Map<Integer, Long> fragmentRowCounts = new HashMap<>(fragments.size());
      Map<Integer, Long> fragmentByteSizes = new HashMap<>(fragments.size());
      for (Fragment fragment : fragments) {
        int id = fragment.getId();
        splits.add(new LanceSplit(Collections.singletonList(id)));
        fragmentRowCounts.put(id, fragment.metadata().getNumRows());
        fragmentByteSizes.put(id, sumDataFileBytes(fragment));
      }
      long resolvedVersion = dataset.getVersion().getId();
      return new ScanPlanResult(splits, resolvedVersion, fragmentRowCounts, fragmentByteSizes);
    } finally {
      if (!open.cached()) {
        dataset.close();
      }
    }
  }

  /**
   * Sums {@link DataFile#getFileSizeBytes()} across the fragment's data files. Returns {@code null}
   * when any file lacks a recorded size — bin-packing without complete sizing is unsafe, so the
   * downstream coalescer treats {@code null} as a hard 1:1 fallback.
   */
  private static Long sumDataFileBytes(Fragment fragment) {
    List<DataFile> files = fragment.metadata().getFiles();
    if (files == null || files.isEmpty()) {
      return null;
    }
    long total = 0L;
    for (DataFile file : files) {
      Long size = file.getFileSizeBytes();
      if (size == null) {
        return null;
      }
      total += size;
    }
    return total;
  }

  /**
   * @deprecated Use {@link #planScan(LanceSparkReadOptions)} instead to get resolved version.
   */
  @Deprecated
  public static List<LanceSplit> generateLanceSplits(LanceSparkReadOptions readOptions) {
    return planScan(readOptions).getSplits();
  }
}
