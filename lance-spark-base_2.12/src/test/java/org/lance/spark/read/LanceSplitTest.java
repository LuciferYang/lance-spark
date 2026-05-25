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

import org.lance.spark.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LanceSplitTest {

  @Test
  public void testPlanScanReturnsNonEmptySplits() {
    LanceSplit.ScanPlanResult result = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    assertFalse(result.getSplits().isEmpty());
    assertTrue(result.getResolvedVersion() > 0);
  }

  @Test
  public void testPlanScanEachSplitHasSingleFragment() {
    LanceSplit.ScanPlanResult result = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    for (LanceSplit split : result.getSplits()) {
      assertEquals(1, split.getFragments().size());
    }
  }

  @Test
  public void testPlanScanReturnsFragmentRowCounts() {
    LanceSplit.ScanPlanResult result = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    assertFalse(result.getFragmentRowCounts().isEmpty());
    // Every fragment in splits should have a row count entry
    for (LanceSplit split : result.getSplits()) {
      for (int fragmentId : split.getFragments()) {
        assertTrue(
            result.getFragmentRowCounts().containsKey(fragmentId),
            "Missing row count for fragment " + fragmentId);
        assertTrue(
            result.getFragmentRowCounts().get(fragmentId) >= 0,
            "Row count should be non-negative for fragment " + fragmentId);
      }
    }
  }

  @Test
  public void testPlanScanReturnsFragmentByteSizes() {
    LanceSplit.ScanPlanResult result = LanceSplit.planScan(TestUtils.TestTable1Config.readOptions);
    // Every fragment must have a byte-size entry (value may be null if the underlying DataFile
    // pre-dates fileSizeBytes tracking — the packaged test fixture is one such case, which is
    // exactly why the coalescer falls back to 1:1 when sizes are unknown).
    assertFalse(result.getFragmentByteSizes().isEmpty());
    for (LanceSplit split : result.getSplits()) {
      for (int fragmentId : split.getFragments()) {
        assertTrue(
            result.getFragmentByteSizes().containsKey(fragmentId),
            "Missing byte size entry for fragment " + fragmentId);
        Long size = result.getFragmentByteSizes().get(fragmentId);
        if (size != null) {
          assertTrue(size > 0, "Byte size should be positive for fragment " + fragmentId);
        }
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGenerateLanceSplitsDeprecated() {
    List<LanceSplit> splits =
        LanceSplit.generateLanceSplits(TestUtils.TestTable1Config.readOptions);
    assertFalse(splits.isEmpty());
  }
}
