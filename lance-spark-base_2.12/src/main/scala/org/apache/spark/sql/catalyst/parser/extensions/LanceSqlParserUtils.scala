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
package org.apache.spark.sql.catalyst.parser.extensions

/**
 * Shared SQL pre-processing helpers used by every version-specific
 * `LanceSparkSqlExtensionsParser` (Spark 3.4 / 3.5 / 4.0 / 4.1). The parser class itself differs
 * across Spark versions (`ParserInterface` evolved), but these helpers are pure string
 * operations and benefit from a single source of truth — every fix (new priority keyword,
 * extended comment syntax, CR/LF tweak) lands in one file instead of four.
 */
object LanceSqlParserUtils {

  /**
   * Returns true when the first non-comment, non-whitespace token of `sqlText` is the start of
   * a Lance extension command that COLLIDES with a Spark built-in. Currently the only
   * collision is {@code ANALYZE TABLE}: Spark's parser succeeds on it (producing
   * {@code AnalyzeTableCommand}), and the analyzer then rejects it for V2 tables. Routing it
   * to the Lance grammar before the delegate is the only way to keep the feature functional.
   *
   * <p>Skips leading whitespace, SQL line comments ({@code -- … LF/CR}), and SQL block
   * comments ({@code SLASH-STAR ... STAR-SLASH}) before the keyword check. {@code ANALYZED} and similar
   * identifier-prefix collisions are explicitly excluded by checking that the keyword is
   * followed by EOF or a non-identifier character.
   */
  def isLanceExtensionPriorityCommand(sqlText: String): Boolean = {
    if (sqlText == null) return false
    val firstTokenStart = skipLeadingTrivia(sqlText)
    firstTokenStart < sqlText.length &&
    startsWithKeywordSequence(sqlText, firstTokenStart, "ANALYZE", "TABLE")
  }

  /**
   * Returns `sqlText` with all leading whitespace, line comments ({@code -- … LF/CR}), and
   * block comments ({@code SLASH-STAR ... STAR-SLASH}) stripped. Used by the priority-routing branch of every
   * `parsePlan` to feed the Lance grammar's lexer (which has no WS/COMMENT skip rule) a clean
   * keyword token. Returns the input unchanged when no leading trivia is present.
   */
  def stripLeadingTrivia(sqlText: String): String = {
    if (sqlText == null) return sqlText
    val start = skipLeadingTrivia(sqlText)
    if (start == 0) sqlText else sqlText.substring(start)
  }

  /**
   * Cap and CR/LF/tab-strip a SQL text before embedding it in an exception message. The text is
   * user-supplied and may flow through Spark's analyzer to log sinks; SLF4J's {@code {}}
   * interpolation does not strip control characters. Truncates to 256 characters with an
   * ellipsis suffix to bound exception message size.
   */
  def sanitizeSqlForError(sqlText: String): String = {
    if (sqlText == null) return null
    val capped =
      if (sqlText.length > 256) sqlText.substring(0, 256) + "…" else sqlText
    capped.replace('\r', '_').replace('\n', '_').replace('\t', '_')
  }

  /**
   * Returns the first index of `s` past any leading whitespace and SQL comments. Line comments
   * terminate at LF or CR (covers Unix LF, Windows CRLF, and classic-Mac CR). Unterminated
   * block comments consume to end-of-input — the safe direction, as a return of `s.length`
   * causes `isLanceExtensionPriorityCommand` to return false.
   */
  private def skipLeadingTrivia(s: String): Int = {
    val n = s.length
    var i = 0
    var advanced = true
    while (advanced && i < n) {
      advanced = false
      val c = s.charAt(i)
      if (Character.isWhitespace(c)) {
        i += 1
        advanced = true
      } else if (c == '-' && i + 1 < n && s.charAt(i + 1) == '-') {
        i += 2
        while (i < n && s.charAt(i) != '\n' && s.charAt(i) != '\r') i += 1
        advanced = true
      } else if (c == '/' && i + 1 < n && s.charAt(i + 1) == '*') {
        i += 2
        while (i + 1 < n && !(s.charAt(i) == '*' && s.charAt(i + 1) == '/')) i += 1
        i = math.min(i + 2, n)
        advanced = true
      }
    }
    i
  }

  /**
   * Match `keywords` (case-insensitive) starting at `start` in `s`, separated by at least one
   * whitespace character, and followed by either end-of-input or a non-identifier character
   * (so `ANALYZED` is not mis-matched as `ANALYZE` + `D`).
   */
  private def startsWithKeywordSequence(s: String, start: Int, keywords: String*): Boolean = {
    var i = start
    val n = s.length
    var k = 0
    while (k < keywords.length) {
      val kw = keywords(k)
      if (i + kw.length > n) return false
      var j = 0
      while (j < kw.length) {
        if (Character.toUpperCase(s.charAt(i + j)) != kw.charAt(j)) return false
        j += 1
      }
      i += kw.length
      if (k < keywords.length - 1) {
        if (i >= n || !Character.isWhitespace(s.charAt(i))) return false
        while (i < n && Character.isWhitespace(s.charAt(i))) i += 1
      } else {
        if (i < n) {
          val next = s.charAt(i)
          if (Character.isLetterOrDigit(next) || next == '_') return false
        }
      }
      k += 1
    }
    true
  }
}
