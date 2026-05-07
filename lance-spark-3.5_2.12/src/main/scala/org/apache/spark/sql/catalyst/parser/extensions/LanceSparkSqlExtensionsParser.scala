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

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

class LanceSparkSqlExtensionsParser(delegate: ParserInterface) extends ParserInterface {

  private lazy val astBuilder = new LanceSqlExtensionsAstBuilder(delegate)

  /**
   * Parse a string to a DataType.
   */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  /**
   * Parse a string to a raw DataType without CHAR/VARCHAR replacement.
   */
  def parseRawDataType(sqlText: String): DataType = throw new UnsupportedOperationException(
    "parseRawDataType is not supported by the Lance SQL extensions parser; use parseDataType instead.")

  /**
   * Parse a string to an Expression.
   */
  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  /**
   * Parse a string to a TableIdentifier.
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  /**
   * Parse a string to a FunctionIdentifier.
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  /**
   * Parse a string to a multi-part identifier.
   */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  /**
   * Parse a string to a LogicalPlan.
   *
   * <p>For commands that exist in Spark's built-in parser AND in our extension grammar (currently
   * just {@code ANALYZE TABLE}), we MUST try the extension grammar first — otherwise Spark's
   * delegate parses {@code ANALYZE TABLE foo COMPUTE STATISTICS} into its own
   * {@code AnalyzeTableCommand}, which the analyzer then rejects for V2 tables with
   * NOT_SUPPORTED_COMMAND_FOR_V2_TABLE before our planner strategy ever runs. For all other
   * statements (Lance-specific commands, plain DML, etc.) we delegate first and only fall through
   * to the extension grammar on parse failure.
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    // Strip leading SQL trivia before the CREATE INDEX prefix check so hinted SQL like
    // `/* hint */ CREATE INDEX ...` is also caught (sqlText.trim only handles whitespace).
    val noTrivia = LanceSparkSqlExtensionsParser.stripLeadingTrivia(sqlText)
    if (noTrivia != null && noTrivia.toUpperCase(java.util.Locale.ROOT).startsWith(
        "CREATE INDEX")) {
      throw new UnsupportedOperationException(
        "Lance does not support standard CREATE INDEX syntax. " +
          "Use: ALTER TABLE <table> CREATE INDEX <name> USING <method> (<columns>)")
    }
    if (LanceSparkSqlExtensionsParser.isLanceExtensionPriorityCommand(sqlText)) {
      // The Lance extension grammar has no whitespace/comment skip rule; pre-strip leading
      // trivia (whitespace + `--` line comments + `/* */` block comments) so the lexer sees a
      // clean ANALYZE token. Without this, hinted SQL like `/* hint */ ANALYZE TABLE …`
      // produces a token-recognition error and a null AST.
      //
      // No catch + delegate fallback here: the predicate has already classified this SQL as a
      // Lance extension command, so any parse failure is a real Lance grammar error (typo in
      // FOR COLUMNS list, etc.) and MUST surface to the caller. Delegating the original SQL to
      // Spark's parser would re-trigger the very V2-rejection failure mode the priority
      // routing was created to prevent.
      parse(LanceSparkSqlExtensionsParser.stripLeadingTrivia(sqlText))
    } else {
      try {
        delegate.parsePlan(sqlText)
      } catch {
        case _: ParseException => parse(sqlText)
      }
    }
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    delegate.parseQuery(sqlText)
  }

  protected def parse(command: String): LogicalPlan = {
    val lexer =
      new LanceSqlExtensionsLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new LanceSqlExtensionsParser(tokenStream)
    parser.removeErrorListeners()

    val visited =
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        astBuilder.visit(parser.singleStatement())
      } catch {
        case _: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          astBuilder.visit(parser.singleStatement())
      }
    if (visited == null) {
      // ANTLR's visitor returns null when no visitor method matches the produced node. The
      // unsafe asInstanceOf below would happily produce a null LogicalPlan; surface a clear
      // parse error instead so the failure does not appear as an NPE in the analyzer phase.
      throw new ParseCancellationException(
        "Lance SQL extensions parser did not produce a LogicalPlan for: " +
          LanceSqlParserUtils.sanitizeSqlForError(command))
    }
    visited.asInstanceOf[LogicalPlan]
  }
}

object LanceSparkSqlExtensionsParser {

  /** @see LanceSqlParserUtils#isLanceExtensionPriorityCommand */
  def isLanceExtensionPriorityCommand(sqlText: String): Boolean =
    LanceSqlParserUtils.isLanceExtensionPriorityCommand(sqlText)

  /** @see LanceSqlParserUtils#stripLeadingTrivia */
  def stripLeadingTrivia(sqlText: String): String =
    LanceSqlParserUtils.stripLeadingTrivia(sqlText)
}

/* Copied from Apache Spark's to avoid dependency on Spark Internals */
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume

  override def getSourceName(): String = wrapped.getSourceName

  override def index(): Int = wrapped.index

  override def mark(): Int = wrapped.mark

  override def release(marker: Int): Unit = wrapped.release(marker)

  override def seek(where: Int): Unit = wrapped.seek(where)

  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  // scalastyle:off
  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
  // scalastyle:on
}
