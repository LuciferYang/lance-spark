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
package org.apache.spark.sql.catalyst.parser.extensions;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LanceSparkSqlExtensionsParser#isLanceExtensionPriorityCommand}. The
 * predicate decides whether a SQL statement should be routed to the Lance grammar BEFORE Spark's
 * delegate — the only correctness gate keeping ANALYZE TABLE on V2 tables out of Spark's
 * NOT_SUPPORTED_COMMAND_FOR_V2_TABLE rejection. A regression here silently breaks the entire
 * feature, so we exercise it against the obvious shapes operators send.
 */
class LanceSparkSqlExtensionsParserPredicateTest {

  private static boolean matches(String sql) {
    return LanceSparkSqlExtensionsParser.isLanceExtensionPriorityCommand(sql);
  }

  @Test
  @DisplayName("Plain ANALYZE TABLE matches (case variants)")
  void plainAnalyzeTable() {
    assertTrue(matches("ANALYZE TABLE foo COMPUTE STATISTICS"));
    assertTrue(matches("analyze table foo compute statistics"));
    assertTrue(matches("Analyze Table foo Compute Statistics"));
  }

  @Test
  @DisplayName("Leading whitespace / tabs / newlines do not block routing")
  void leadingWhitespace() {
    assertTrue(matches("   ANALYZE TABLE foo"));
    assertTrue(matches("\tANALYZE TABLE foo"));
    assertTrue(matches("\n\nANALYZE TABLE foo"));
  }

  @Test
  @DisplayName("Mixed whitespace between ANALYZE and TABLE is allowed")
  void interKeywordWhitespace() {
    assertTrue(matches("ANALYZE  TABLE foo"));
    assertTrue(matches("ANALYZE\tTABLE foo"));
    assertTrue(matches("ANALYZE\nTABLE foo"));
  }

  @Test
  @DisplayName("Leading SQL line comments are skipped before the keyword check")
  void leadingLineComments() {
    assertTrue(matches("-- a hint\nANALYZE TABLE foo"));
    assertTrue(matches("-- one\n-- two\nANALYZE TABLE foo"));
  }

  @Test
  @DisplayName("Leading SQL block comments are skipped before the keyword check")
  void leadingBlockComments() {
    assertTrue(matches("/* hint */ ANALYZE TABLE foo"));
    assertTrue(matches("/* multi\nline */ANALYZE TABLE foo"));
    assertTrue(matches("/*a*//*b*/ANALYZE TABLE foo"));
  }

  @Test
  @DisplayName("Mixed leading comments + whitespace are skipped")
  void mixedLeadingNoise() {
    assertTrue(matches("  /* hint */ -- and another\n  ANALYZE TABLE foo"));
  }

  @Test
  @DisplayName("Other Spark/Lance commands do NOT match")
  void otherCommandsRejected() {
    assertFalse(matches("SELECT * FROM foo"));
    assertFalse(matches("VACUUM foo"));
    assertFalse(matches("OPTIMIZE foo"));
    assertFalse(matches("DROP TABLE foo"));
  }

  @Test
  @DisplayName("Identifiers that share the keyword prefix do NOT match")
  void identifierPrefixCollision() {
    // ANALYZED is a valid identifier; must not be classified as ANALYZE + D.
    assertFalse(matches("SELECT * FROM ANALYZED"));
    // ANALYZE without TABLE.
    assertFalse(matches("ANALYZE foo"));
    // ANALYZER is one token, not ANALYZE + R + …
    assertFalse(matches("ANALYZER TABLE foo"));
  }

  @Test
  @DisplayName("Null and empty / comment-only inputs are rejected")
  void nullAndEmpty() {
    assertFalse(LanceSparkSqlExtensionsParser.isLanceExtensionPriorityCommand(null));
    assertFalse(matches(""));
    assertFalse(matches("   "));
    assertFalse(matches("/* just a comment */"));
    assertFalse(matches("-- comment only\n"));
  }

  @Test
  @DisplayName("Unterminated block comment safely rejects (no false positive)")
  void unterminatedBlockComment() {
    // The skipper consumes until end-of-input on an unterminated block comment, leaving no
    // non-comment tokens. Safe direction: when in doubt, do not route.
    assertFalse(matches("/* never closed ANALYZE TABLE foo"));
  }

  @Test
  @DisplayName("ANALYZE followed by a different keyword does NOT match")
  void analyzeWithoutTable() {
    // E.g. PostgreSQL's "ANALYZE VERBOSE foo" doesn't have TABLE.
    assertFalse(matches("ANALYZE VERBOSE foo"));
    assertFalse(matches("ANALYZE foo"));
  }
}
