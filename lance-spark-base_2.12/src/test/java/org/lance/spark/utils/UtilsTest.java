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

import org.lance.Version;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for pure-logic methods in {@link Utils}. */
public class UtilsTest {

  @Test
  public void testParseVersion_validValues() {
    assertEquals(0L, Utils.parseVersion("0"));
    assertEquals(1L, Utils.parseVersion("1"));
    assertEquals(42L, Utils.parseVersion("42"));
    assertEquals(Long.MAX_VALUE, Utils.parseVersion(Long.toUnsignedString(Long.MAX_VALUE)));
  }

  @Test
  public void testParseVersion_maxUnsigned() {
    // -1L as unsigned = 18446744073709551615
    long maxUnsigned = -1L;
    String maxUnsignedStr = Long.toUnsignedString(maxUnsigned);
    assertEquals(maxUnsigned, Utils.parseVersion(maxUnsignedStr));
  }

  @Test
  public void testParseVersion_invalidInput() {
    assertThrows(NumberFormatException.class, () -> Utils.parseVersion("abc"));
    assertThrows(NumberFormatException.class, () -> Utils.parseVersion(""));
    assertThrows(NumberFormatException.class, () -> Utils.parseVersion("-1"));
  }

  /**
   * Creates a Version with a timestamp expressed as a literal ZonedDateTime, avoiding
   * re-implementing the production micros-to-Instant conversion inside the test helper.
   *
   * @param epochSeconds seconds since epoch (e.g. 1 for 1970-01-01T00:00:01Z)
   */
  private static Version version(long id, long epochSeconds) {
    ZonedDateTime dateTime =
        ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).plusSeconds(epochSeconds);
    return new Version(id, dateTime, new TreeMap<>());
  }

  @Test
  public void testFindVersion_singleVersion() {
    // version at epoch second 1 → timestamp 1_000_000 micros
    List<Version> versions = Collections.singletonList(version(1, 1));
    assertEquals(1L, Utils.findVersion(versions, 1_000_000L));
  }

  @Test
  public void testFindVersion_exactMatch() {
    List<Version> versions = Arrays.asList(version(1, 1), version(2, 2), version(3, 3));
    // 2_000_000 micros = epoch second 2 → exact match on v2
    assertEquals(2L, Utils.findVersion(versions, 2_000_000L));
  }

  @Test
  public void testFindVersion_betweenVersions() {
    List<Version> versions = Arrays.asList(version(1, 1), version(2, 3));
    // 2_000_000 micros (second 2) is between v1(1s) and v2(3s) → returns v1
    assertEquals(1L, Utils.findVersion(versions, 2_000_000L));
  }

  @Test
  public void testFindVersion_afterAllVersions() {
    List<Version> versions = Arrays.asList(version(1, 1), version(2, 2));
    // 5_000_000 micros (second 5) is after all versions → returns last version
    assertEquals(2L, Utils.findVersion(versions, 5_000_000L));
  }

  @Test
  public void testFindVersion_beforeAllVersions_throws() {
    List<Version> versions = Collections.singletonList(version(1, 2));
    // 1_000_000 micros (second 1) is before v1(2s) → throws
    assertThrows(IllegalArgumentException.class, () -> Utils.findVersion(versions, 1_000_000L));
  }

  @Test
  public void testFindVersion_emptyList_throws() {
    List<Version> versions = Collections.emptyList();
    assertThrows(IllegalArgumentException.class, () -> Utils.findVersion(versions, 1_000_000L));
  }

  @Test
  public void testFindVersion_negativeTimestamp_throws() {
    List<Version> versions = Collections.singletonList(version(1, 1));
    assertThrows(IllegalArgumentException.class, () -> Utils.findVersion(versions, -1L));
  }

  @Test
  public void testFindVersion_zeroTimestamp_throws() {
    List<Version> versions = Collections.singletonList(version(1, 1));
    assertThrows(IllegalArgumentException.class, () -> Utils.findVersion(versions, 0L));
  }
}
