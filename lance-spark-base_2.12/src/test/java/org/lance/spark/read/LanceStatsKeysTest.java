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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link LanceStatsKeys} key constants and validators. */
class LanceStatsKeysTest {

  @Test
  @DisplayName("validateColumnName accepts plain names")
  void validateColumnNameAccepts() {
    LanceStatsKeys.validateColumnName("foo");
    LanceStatsKeys.validateColumnName("foo_bar");
    LanceStatsKeys.validateColumnName("Mixed-Case");
    LanceStatsKeys.validateColumnName("with spaces");
    // No assertion needed — passing without throwing is the contract.
  }

  @Test
  @DisplayName("validateColumnName rejects dotted names (key ambiguity with nested-leaf paths)")
  void validateColumnNameRejectsDots() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> LanceStatsKeys.validateColumnName("a.b"));
    assertTrue(ex.getMessage().contains("'.'"));
  }

  @Test
  @DisplayName("validateColumnName rejects null")
  void validateColumnNameRejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> LanceStatsKeys.validateColumnName(null));
  }

  @Test
  @DisplayName("validateColumnName rejects empty string (orphan-cleanup ambiguity)")
  void validateColumnNameRejectsEmpty() {
    assertThrows(IllegalArgumentException.class, () -> LanceStatsKeys.validateColumnName(""));
  }

  @Test
  @DisplayName("isStatsCompatibleColumnName: empty / null / dotted all reject")
  void isStatsCompatibleColumnNameRejectsBadInputs() {
    assertFalse(LanceStatsKeys.isStatsCompatibleColumnName(null));
    assertFalse(LanceStatsKeys.isStatsCompatibleColumnName(""));
    assertFalse(LanceStatsKeys.isStatsCompatibleColumnName("a.b"));
    assertTrue(LanceStatsKeys.isStatsCompatibleColumnName("a"));
  }

  @Test
  @DisplayName("COLUMN_KEY_REGEX captures top-level column name")
  void columnKeyRegexTopLevel() {
    Pattern p = Pattern.compile(LanceStatsKeys.COLUMN_KEY_REGEX);
    Matcher m = p.matcher("lance.stats.column.foo.min");
    assertTrue(m.matches());
    assertEquals("foo", m.group(1));
  }

  @Test
  @DisplayName("COLUMN_KEY_REGEX captures dotted column name (orphan-cleanup forward compat)")
  void columnKeyRegexDottedCapture() {
    // Today's writer rejects dotted names, but legacy props may exist; orphan cleanup still
    // needs to remove them. The regex anchors on the closed suffix set so dotted captures are
    // by construction orphan keys for columns no longer in the schema.
    Pattern p = Pattern.compile(LanceStatsKeys.COLUMN_KEY_REGEX);
    Matcher m = p.matcher("lance.stats.column.a.b.min");
    assertTrue(m.matches());
    assertEquals("a.b", m.group(1));
  }

  @Test
  @DisplayName("COLUMN_KEY_REGEX rejects non-suffix keys")
  void columnKeyRegexRejectsUnknownSuffix() {
    Pattern p = Pattern.compile(LanceStatsKeys.COLUMN_KEY_REGEX);
    assertFalse(p.matcher("lance.stats.column.foo.unknown").matches());
    assertFalse(p.matcher("lance.stats.numRows").matches());
    assertFalse(p.matcher("foo.lance.stats.column.bar.min").matches());
  }

  @Test
  @DisplayName("Concatenated keys equal top-level constants")
  void keyConstantsConsistency() {
    assertEquals(
        "lance.stats.column.foo.min",
        LanceStatsKeys.COLUMN_PREFIX + "foo" + LanceStatsKeys.COLUMN_SUFFIX_MIN);
    assertEquals(
        "lance.stats.column.foo.distinctCount",
        LanceStatsKeys.COLUMN_PREFIX + "foo" + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_COUNT);
  }
}
