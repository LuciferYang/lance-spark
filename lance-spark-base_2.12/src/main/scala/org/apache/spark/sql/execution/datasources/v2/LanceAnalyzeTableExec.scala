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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LanceAnalyzeTableOutputType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.types.{AnsiIntervalType, ArrayType, CalendarIntervalType, DataType, MapType, NullType, StructField, StructType, UserDefinedType}
import org.lance.spark.LanceDataset
import org.lance.spark.read.{ColumnStatsCodec, LanceStatsKeys}
import org.lance.spark.shim.LanceCommandUtilsShim

import scala.collection.JavaConverters._

/**
 * Executor for `ANALYZE TABLE`. Delegates basic per-column statistics computation
 * ({@code min}, {@code max}, {@code nullCount}, HLL-approx {@code distinctCount}, {@code avgLen},
 * {@code maxLen}, optional equi-height histogram) to Spark's
 * {@code CommandUtils.computeColumnStats} — the same engine Spark's native V1 ANALYZE TABLE uses.
 * For the user-facing {@code approx=false} mode (which Spark's CommandUtils does not support;
 * its NDV path hardcodes HyperLogLogPlusPlus), a second physical aggregation computes exact
 * {@code countDistinct} and overrides the HLL value before persistence.
 *
 * <p>Results are written to the Lance table's properties under
 * {@code lance.stats.column.<name>.{min,max,nullCount,distinctCount,distinctMode,avgLen,maxLen,
 * histogram,histogramFormat}}, plus the table-level {@code lance.stats.computedAtVersion} =
 * current Lance manifest version. {@link org.lance.spark.read.LanceScanBuilder} reads these on
 * subsequent scans, eliminating the per-scan zonemap I/O cost from Phase 1.
 *
 * <p>Encoding of typed min/max into string-typed table properties is delegated to
 * {@code ColumnStatsCodec} in lance-spark-base.
 */
case class LanceAnalyzeTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    columns: Seq[String],
    forAllColumns: Boolean,
    approx: Boolean) extends LeafV2CommandExec with Logging {

  override def output: Seq[Attribute] = LanceAnalyzeTableOutputType.SCHEMA

  /**
   * Returns true when Spark's `min`/`max` aggregations support ordering on this type AND the
   * codec has an encoder for it. Complex containers (StructType, ArrayType, MapType,
   * UserDefinedType, NullType) raise AnalysisException at planning time inside
   * `df.agg(min(col), max(col))`. Interval types (CalendarIntervalType, AnsiIntervalType — the
   * parent of YearMonthIntervalType / DayTimeIntervalType) have no entry in
   * {@link org.lance.spark.read.ColumnStatsCodec}, so encode would silently return null and
   * drop the stat — better to skip up front with a log line than mislead the operator that
   * stats were computed when none were persisted.
   */
  private def isStatSupported(dt: DataType): Boolean = dt match {
    case _: StructType | _: ArrayType | _: MapType | _: UserDefinedType[_] | _: NullType => false
    case _: CalendarIntervalType | _: AnsiIntervalType => false
    case _ => true
  }

  override protected def run(): Seq[InternalRow] = {
    // Sanitized table name for all subsequent error / log lines (CR/LF stripped, length capped).
    val tableName = LanceAnalyzeTableExec.sanitizeForLog(ident.toString)

    // The persisted-stats payload requires single-manifest atomicity: BaseLanceNamespace-
    // SparkCatalog issues one Dataset.updateConfig per alterTable call. Other catalogs may
    // batch differently, leaving complete=false on disk permanently — reject up front.
    if (!catalog.isInstanceOf[org.lance.spark.BaseLanceNamespaceSparkCatalog]) {
      throw new UnsupportedOperationException(
        s"ANALYZE TABLE $tableName requires a BaseLanceNamespaceSparkCatalog (got " +
          catalog.getClass.getName + "). The atomicity contract for the persisted-stats " +
          "wire format depends on the catalog committing the change list as a single manifest " +
          "write, which other TableCatalog implementations do not guarantee.")
    }
    val lanceDataset = catalog.loadTable(ident) match {
      case ds: LanceDataset => ds
      case other =>
        throw new UnsupportedOperationException(
          s"ANALYZE TABLE $tableName: only LanceDataset tables are supported; got " +
            other.getClass.getName)
    }

    val schema: StructType = lanceDataset.schema()
    // Reject duplicate field names BEFORE type/name-shape filters: persisted-stats keys would
    // collide regardless of column type, so this is the most fundamental constraint and should
    // surface first.
    val duplicateNames = schema.fields.groupBy(_.name).collect {
      case (name, fields) if fields.length > 1 => name
    }
    if (duplicateNames.nonEmpty) {
      throw new IllegalArgumentException(
        s"ANALYZE TABLE $tableName: schema has duplicate field names: " +
          duplicateNames.toSeq.sorted.mkString(", ") +
          ". Persisted-stats keys would collide; rename the conflicting columns first.")
    }
    val requestedCols: Seq[StructField] =
      if (forAllColumns) schema.fields.toSeq
      else {
        val byName = schema.fields.map(f => f.name -> f).toMap
        columns.map(c =>
          byName.getOrElse(
            c,
            throw new IllegalArgumentException(
              s"ANALYZE TABLE $tableName: column not found: $c")))
      }
    // Filter complex types (Struct/Array/Map/UDT/Null) and interval types: df.agg(min(col))
    // raises AnalysisException at planning time on these. FOR ALL COLUMNS skips silently to
    // avoid crashing ANALYZE on tables that happen to contain one. FOR COLUMNS fails fast.
    val (typeSupported, complexSkipped) = requestedCols.partition(f => isStatSupported(f.dataType))
    if (complexSkipped.nonEmpty) {
      // Sanitize per-name BEFORE mkString so the per-name length cap is honored and a hostile
      // name can't inject CR/LF into either log lines or the driver query-error response.
      val sanitizedSkippedNames =
        complexSkipped.map(f => LanceAnalyzeTableExec.sanitizeForLog(f.name)).mkString(", ")
      if (forAllColumns) {
        log.info(
          s"ANALYZE TABLE $tableName skipping unsupported-type columns: $sanitizedSkippedNames")
      } else {
        throw new IllegalArgumentException(
          s"ANALYZE TABLE $tableName FOR COLUMNS does not support complex types (StructType / " +
            s"ArrayType / MapType / UserDefinedType / NullType). Unsupported columns: " +
            sanitizedSkippedNames)
      }
    }

    // Reject names that would produce ambiguous stats keys: empty (collides with the orphan-
    // cleanup regex) or containing '.' (collides with future nested-leaf paths). FOR-ALL skips
    // and logs (separately for empty vs dotted, so operators see the actual cause); FOR-COLUMNS
    // delegates to validateColumnName for a fail-fast exception with the canonical message.
    val (targetCols, nameSkipped) =
      typeSupported.partition(f => LanceStatsKeys.isStatsCompatibleColumnName(f.name))
    if (nameSkipped.nonEmpty) {
      if (forAllColumns) {
        val (emptyNames, dottedNames) = nameSkipped.partition(_.name.isEmpty)
        if (emptyNames.nonEmpty) {
          log.info(s"ANALYZE TABLE skipping ${emptyNames.size} column(s) with empty names " +
            "(persisted-stats keys would collide with the orphan-cleanup regex).")
        }
        if (dottedNames.nonEmpty) {
          // Per-name sanitize before join (see complexSkipped path above for rationale).
          val sanitizedNames =
            dottedNames.map(f => LanceAnalyzeTableExec.sanitizeForLog(f.name)).mkString(", ")
          log.info("ANALYZE TABLE skipping columns whose names contain '.' (key ambiguity " +
            "with nested-leaf paths): " + sanitizedNames)
        }
      } else {
        // FOR COLUMNS must fail fast. Use the first bad name as the exception's example;
        // validateColumnName picks the right message for empty vs dotted.
        LanceStatsKeys.validateColumnName(nameSkipped.head.name)
      }
    }

    // SparkSession.active reads a ThreadLocal; under AQE re-plan the calling thread may have
    // no session bound. Wrap the generic IllegalStateException with a command-specific message.
    val spark =
      try {
        SparkSession.active
      } catch {
        case _: IllegalStateException =>
          throw new IllegalStateException(
            "ANALYZE TABLE requires an active SparkSession on the calling thread; ensure the " +
              "command runs from the driver query thread (AQE re-plan threads may not have a " +
              "session bound).")
      }
    // If every requested column is unsupported (e.g., a struct-only table under FOR ALL COLUMNS),
    // there is nothing to analyze. Return an empty result rather than write a stats payload of
    // just version / numRows / hash with no per-column entries. The complete=false sentinel is
    // never set in this branch, so any pre-existing valid stats are preserved.
    //
    // Output-row contract for this branch: columns_analyzed=0, num_rows=0, manifest_version=0,
    // approx=<input>. The manifest_version=0 here is a sentinel meaning "no write occurred";
    // the table's actual manifest version is unchanged. Tooling that reads the command output
    // should use columns_analyzed==0 as the signal that no stats were persisted, not the
    // manifest_version value.
    if (targetCols.isEmpty) {
      log.info(
        s"ANALYZE TABLE $tableName: no analyzable columns " +
          s"(${requestedCols.size} requested, all unsupported types) — skipping write.")
      return Seq(new GenericInternalRow(Array[Any](
        Integer.valueOf(0),
        java.lang.Long.valueOf(0L),
        java.lang.Long.valueOf(0L),
        java.lang.Boolean.valueOf(approx))))
    }

    val df = spark.read.format("lance").load(lanceDataset.readOptions().getDatasetUri)

    // Capture the manifest version BEFORE the aggregation read. A concurrent writer between
    // this line and alterTable means the stats reflect a version the table never directly
    // held; the read path's exact-equality check then rejects them and falls back to live
    // aggregation. Safer-stale-than-wrong.
    val readVersion = {
      val ds = org.lance.spark.utils.Utils.openDatasetBuilder(lanceDataset.readOptions()).build()
      try {
        ds.getVersion.getId
      } finally {
        ds.close()
      }
    }

    // Session resolver mirrors Spark's case-sensitivity. Duplicate-name rejection earlier
    // guarantees at most one match per name.
    val resolver = spark.sessionState.conf.resolver
    val relation = df.queryExecution.analyzed
    val attrs: Seq[Attribute] = targetCols.flatMap { f =>
      relation.output.find(a => resolver(a.name, f.name))
    }

    // Both flags must be true to compute AND persist histograms. Decoupled from Spark's global
    // flag so a cluster-wide flip for HMS-backed tables doesn't bloat Lance manifests.
    // java.lang.Boolean.parseBoolean (lenient) matches existing Lance config readers.
    val lanceHistogramEnabled = spark.conf
      .getOption(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_HISTOGRAM_ENABLED)
      .map(s => java.lang.Boolean.parseBoolean(s.trim))
      .getOrElse(LanceStatsKeys.DEFAULT_CBO_COLUMN_STATS_HISTOGRAM_ENABLED)
    val sparkHistogramEnabled = spark.sessionState.conf.histogramEnabled
    val cacheRelation = !approx && spark.conf
      .getOption(LanceStatsKeys.SPARK_CONF_CBO_COLUMN_STATS_CACHE_ENABLED)
      .map(s => java.lang.Boolean.parseBoolean(s.trim))
      .getOrElse(LanceStatsKeys.DEFAULT_CBO_COLUMN_STATS_CACHE_ENABLED)

    // Cache around BOTH passes when approx=false: Spark substitutes InMemoryRelation for
    // pass 2 after pass 1 materializes — net 1 scan instead of 2.
    val cachedDf: Option[DataFrame] = if (cacheRelation) {
      df.cache()
      Some(df)
    } else None

    // Hoisted outside the try so the post-try complete=true write can append to the same buffer.
    val changes = scala.collection.mutable.ArrayBuffer[TableChange]()

    val (rowCount, manifestVersion, computedAt, schemaHash) =
      try {
        // CommandUtils is private[sql]; our package o.a.s.sql.execution.datasources.v2 has access.
        // Signature is stable across Spark 3.4 / 3.5 / 4.0 / 4.1.
        val (rc, colStatsMap): (Long, Map[Attribute, ColumnStat]) =
          LanceCommandUtilsShim.computeColumnStats(spark, relation, attrs)

        // CommandUtils hardcodes HyperLogLogPlusPlus for NDV; override with countDistinct
        // when approx=false. The cache (when enabled) makes this hit InMemoryRelation.
        val exactNdvByAttr: Map[Attribute, Long] = if (!approx) {
          val ndvAggs = attrs.map(a => countDistinct(df.col(a.name)).as(s"__exact_ndv__${a.name}"))
          if (ndvAggs.isEmpty) {
            Map.empty
          } else {
            val row = df.agg(ndvAggs.head, ndvAggs.tail: _*).collect().head
            attrs.zipWithIndex.map { case (a, i) =>
              a -> (if (row.isNullAt(i)) 0L else row.getLong(i))
            }.toMap
          }
        } else {
          Map.empty
        }

        val mv = readVersion + 1
        val ca = java.time.Instant.now().toString
        val sh = LanceStatsKeys.computeSchemaHash(schema)

        // Atomicity protocol: complete=false first, then orphan GC, then table+column stats,
        // then complete=true (after try). One alterTable commits atomically; last-write-wins
        // collapses the duplicate `complete` keys to true.
        changes += TableChange.setProperty(LanceStatsKeys.COMPLETE, "false")

        val currentSchemaNames: Set[String] = schema.fields.map(_.name).toSet
        val statsColumnKeyRe = LanceAnalyzeTableExec.STATS_COLUMN_KEY_RE
        val existingProps: Map[String, String] =
          try {
            Option(lanceDataset.properties()).map(_.asScala.toMap).getOrElse(Map.empty)
          } catch {
            case _: Exception => Map.empty[String, String]
          }
        val orphanKeys = existingProps.keys.collect {
          case k @ statsColumnKeyRe(colName) if !currentSchemaNames.contains(colName) => k
        }.toSeq
        orphanKeys.foreach(k => changes += TableChange.removeProperty(k))
        val orphansRemoved = orphanKeys.size
        if (orphansRemoved > 0) {
          log.info(
            s"ANALYZE TABLE $tableName: removing $orphansRemoved orphan column-stats key(s) for " +
              s"columns no longer in schema")
        }

        changes += TableChange.setProperty(LanceStatsKeys.VERSION, LanceStatsKeys.SUPPORTED_VERSION)
        changes += TableChange.setProperty(LanceStatsKeys.COMPUTED_AT_VERSION, mv.toString)
        changes += TableChange.setProperty(LanceStatsKeys.NUM_ROWS, rc.toString)
        changes += TableChange.setProperty(LanceStatsKeys.COMPUTED_AT, ca)
        changes += TableChange.setProperty(LanceStatsKeys.SCHEMA_HASH, sh)

        attrs.foreach { attr =>
          // CommandUtils may drop columns whose type it can't handle; .get + foreach prevents
          // one bad column from failing the whole ANALYZE.
          colStatsMap.get(attr).foreach { cs =>
            val keyBase = LanceStatsKeys.COLUMN_PREFIX + attr.name

            cs.min.foreach { v =>
              val enc = ColumnStatsCodec.encode(v, attr.dataType)
              if (enc != null) {
                changes += TableChange.setProperty(keyBase + LanceStatsKeys.COLUMN_SUFFIX_MIN, enc)
              }
            }
            cs.max.foreach { v =>
              val enc = ColumnStatsCodec.encode(v, attr.dataType)
              if (enc != null) {
                changes += TableChange.setProperty(keyBase + LanceStatsKeys.COLUMN_SUFFIX_MAX, enc)
              }
            }

            cs.nullCount.foreach { n =>
              changes += TableChange.setProperty(
                keyBase + LanceStatsKeys.COLUMN_SUFFIX_NULL_COUNT,
                n.toString)
            }

            // distinctCount=0 (all-null column) must be filtered: the read-path rejects ndv <= 0
            // and the catch drops the ENTIRE column's stats, not just NDV.
            val ndvOpt: Option[BigInt] =
              if (approx) cs.distinctCount else exactNdvByAttr.get(attr).map(BigInt(_))
            ndvOpt.filter(_ > 0).foreach { ndv =>
              changes += TableChange.setProperty(
                keyBase + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_COUNT,
                ndv.toString)
              changes += TableChange.setProperty(
                keyBase + LanceStatsKeys.COLUMN_SUFFIX_DISTINCT_MODE,
                if (approx) LanceStatsKeys.DISTINCT_MODE_APPROX
                else LanceStatsKeys.DISTINCT_MODE_EXACT)
            }

            cs.avgLen.foreach { v =>
              changes += TableChange.setProperty(
                keyBase + LanceStatsKeys.COLUMN_SUFFIX_AVG_LEN,
                v.toString)
            }
            cs.maxLen.foreach { v =>
              changes += TableChange.setProperty(
                keyBase + LanceStatsKeys.COLUMN_SUFFIX_MAX_LEN,
                v.toString)
            }

            if (lanceHistogramEnabled && sparkHistogramEnabled) {
              cs.histogram.foreach { h =>
                val encoded = ColumnStatsCodec.encodeHistogram(h)
                if (encoded != null) {
                  changes += TableChange.setProperty(
                    keyBase + LanceStatsKeys.COLUMN_SUFFIX_HISTOGRAM,
                    encoded)
                  changes += TableChange.setProperty(
                    keyBase + LanceStatsKeys.COLUMN_SUFFIX_HISTOGRAM_FORMAT,
                    LanceStatsKeys.HISTOGRAM_FORMAT_V1)
                }
              }
            }
          }
        }

        (rc, mv, ca, sh)
      } finally {
        cachedDf.foreach(_.unpersist())
      }

    // Step 4: complete-marker LAST — only valid stats payloads have lance.stats.complete=true.
    changes += TableChange.setProperty(LanceStatsKeys.COMPLETE, "true")

    // Atomicity: BaseLanceNamespaceSparkCatalog merges the entire `changes` array into one
    // property map and issues a single Dataset.updateConfig — the manifest commits atomically
    // and the duplicate `complete` writes collapse to the final "true" by last-write-wins. The
    // catalog instanceof check above is what enforces this contract; the complete=false-first /
    // complete=true-last ordering documents intent for any future catalog that honored order.
    catalog.alterTable(ident, changes.toArray: _*)
    // Include computedAt + numRows + approx so cron operators can grep logs to answer
    // "when did this table's stats last refresh, and was NDV exact or approximate?".
    log.info(
      s"ANALYZE TABLE $tableName: stats persisted for ${targetCols.size} columns at " +
        s"manifest version $manifestVersion (numRows=$rowCount, approx=$approx, " +
        s"computedAt=$computedAt, schemaHash=${schemaHash.take(8)}…)")

    Seq(new GenericInternalRow(Array[Any](
      targetCols.size.asInstanceOf[java.lang.Integer],
      rowCount.asInstanceOf[java.lang.Long],
      manifestVersion.asInstanceOf[java.lang.Long],
      java.lang.Boolean.valueOf(approx))))
  }
}

object LanceAnalyzeTableExec {

  /**
   * Compiled once at class load. Scala's `Regex` is thread-safe (delegates to a stateless
   * `java.util.regex.Pattern`) so a single instance can serve all concurrent ANALYZE
   * invocations.
   */
  private val STATS_COLUMN_KEY_RE = LanceStatsKeys.COLUMN_KEY_REGEX.r

  /**
   * Strip CR/LF/tab from a value before interpolating into a log message, and bound length.
   * Mirrors {@code LanceScanBuilder.sanitizeForLog} in Java so writer- and reader-side log
   * lines have consistent injection-resistance. Used for any user-controllable input flowing
   * into a Scala s-string log call (table identifier, column name, exception message).
   */
  private[v2] def sanitizeForLog(value: String): String = {
    if (value == null) {
      null
    } else {
      val trimmed = if (value.length > 256) value.substring(0, 256) + "…" else value
      trimmed.replace('\r', '_').replace('\n', '_').replace('\t', '_')
    }
  }
}
