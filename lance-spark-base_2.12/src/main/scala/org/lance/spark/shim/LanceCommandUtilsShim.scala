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
package org.lance.spark.shim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}

import java.lang.reflect.{InvocationTargetException, Method}

/**
 * Cross-version adapter for {@code CommandUtils.computeColumnStats}. Reflection avoids the
 * compile-time {@code SparkSession} type mismatch — Spark 4.x takes
 * {@code classic.SparkSession}, Spark 3.x takes the generic class. The Spark 4 modules share
 * resources with {@code 3.5_2.12}, so a ServiceLoader entry can't disambiguate per-version.
 */
object LanceCommandUtilsShim {

  private val commandUtilsClassName = "org.apache.spark.sql.execution.command.CommandUtils$"

  // Narrow by param count to defend against future overloads; getDeclaredMethods has no
  // defined order (JLS §8.4).
  private val COMPUTE_COLUMN_STATS_PARAM_COUNT = 3

  private lazy val (moduleInstance: AnyRef, computeColumnStatsMethod: Method) = {
    val cls = Class.forName(commandUtilsClassName)
    val module = cls.getField("MODULE$").get(null)
    val candidates = cls.getDeclaredMethods.filter { m =>
      m.getName == "computeColumnStats" && m.getParameterCount == COMPUTE_COLUMN_STATS_PARAM_COUNT
    }
    val method = candidates match {
      case Array(single) => single
      case Array() =>
        throw new IllegalStateException(
          s"CommandUtils.computeColumnStats(_, _, _) not found in $commandUtilsClassName. " +
            "This signals an incompatible Spark version. Lance-spark requires Spark 3.4 or later " +
            "with the standard org.apache.spark.sql.execution.command.CommandUtils.")
      case multiple =>
        throw new IllegalStateException(
          s"CommandUtils.computeColumnStats(_, _, _) is ambiguous in $commandUtilsClassName " +
            s"(${multiple.length} matches). This signals a Spark version with overloaded " +
            "signatures that lance-spark hasn't been audited against; please file an issue.")
    }
    method.setAccessible(true)
    (module, method)
  }

  def computeColumnStats(
      spark: SparkSession,
      relation: LogicalPlan,
      columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) = {
    val result =
      try {
        computeColumnStatsMethod.invoke(
          moduleInstance, spark.asInstanceOf[AnyRef], relation, columns)
      } catch {
        // Unwrap so the caller sees the actual AnalysisException, not reflection noise.
        case e: InvocationTargetException =>
          val cause = e.getCause
          if (cause != null) throw cause else throw e
      }
    result.asInstanceOf[(Long, Map[Attribute, ColumnStat])]
  }
}
