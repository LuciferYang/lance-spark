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
package org.apache.spark.sql.util

/*
 * The following code is originally from https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/util/ArrowUtilsSuite.scala
 * and is licensed under the Apache license:
 *
 * License: Apache License 2.0, Copyright 2014 and onwards The Apache Software Foundation.
 * https://github.com/apache/spark/blob/master/LICENSE
 *
 * It has been modified by the Lance developers to fit the needs of the Lance project.
 */

import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.{Field, FieldType}
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.types._
import org.lance.spark.LanceConstant
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId

class LanceArrowUtilsSuite extends AnyFunSuite {
  def roundtrip(dt: DataType, fieldName: String = "value"): Unit = {
    dt match {
      case schema: StructType =>
        assert(LanceArrowUtils.fromArrowSchema(
          LanceArrowUtils.toArrowSchema(schema, null, true)) === schema)
      case _ =>
        roundtrip(new StructType().add(fieldName, dt))
    }
  }

  test("unsigned") {
    roundtrip(BooleanType, LanceConstant.ROW_ID)
    val arrowType = LanceArrowUtils.toArrowField(LanceConstant.ROW_ID, LongType, true, "Beijing")
    assert(arrowType.getType.asInstanceOf[ArrowType.Int].getBitWidth === 64)
    assert(!arrowType.getType.asInstanceOf[ArrowType.Int].getIsSigned)
    // also verify unsigned smaller integers mapping (uint8/uint16/uint32)
    val u8Field = new Field(
      "u8",
      new FieldType(true, new ArrowType.Int(8, /* signed = */ false), null, null),
      java.util.Collections.emptyList())
    val u16Field = new Field(
      "u16",
      new FieldType(true, new ArrowType.Int(16, /* signed = */ false), null, null),
      java.util.Collections.emptyList())
    val u32Field = new Field(
      "u32",
      new FieldType(true, new ArrowType.Int(32, /* signed = */ false), null, null),
      java.util.Collections.emptyList())
    assert(LanceArrowUtils.fromArrowField(u8Field) === ShortType)
    assert(LanceArrowUtils.fromArrowField(u16Field) === IntegerType)
    assert(LanceArrowUtils.fromArrowField(u32Field) === LongType)
  }

  test("simple") {
    roundtrip(BooleanType)
    roundtrip(ByteType)
    roundtrip(ShortType)
    roundtrip(IntegerType)
    roundtrip(LongType)
    roundtrip(FloatType)
    roundtrip(DoubleType)
    roundtrip(StringType)
    roundtrip(BinaryType)
    roundtrip(DecimalType.SYSTEM_DEFAULT)
    roundtrip(DateType)
    roundtrip(YearMonthIntervalType())
    roundtrip(DayTimeIntervalType())
  }

  test("timestamp") {

    def roundtripWithTz(timeZoneId: String): Unit = {
      val schema = new StructType().add("value", TimestampType)
      val arrowSchema = LanceArrowUtils.toArrowSchema(schema, timeZoneId, true)
      val fieldType = arrowSchema.findField("value").getType.asInstanceOf[ArrowType.Timestamp]
      assert(fieldType.getTimezone() === timeZoneId)
      assert(LanceArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    roundtripWithTz(ZoneId.systemDefault().getId)
    roundtripWithTz("Asia/Tokyo")
    roundtripWithTz("UTC")
  }

  test("array") {
    roundtrip(ArrayType(IntegerType, containsNull = true))
    roundtrip(ArrayType(IntegerType, containsNull = false))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = true))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = false))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = false))
  }

  test("struct") {
    roundtrip(new StructType())
    roundtrip(new StructType().add("i", IntegerType))
    roundtrip(new StructType().add("arr", ArrayType(IntegerType)))
    roundtrip(new StructType().add("i", IntegerType).add("arr", ArrayType(IntegerType)))
    roundtrip(new StructType().add(
      "struct",
      new StructType().add("i", IntegerType).add("arr", ArrayType(IntegerType))))
  }

  test("nested date millisecond types") {
    val dateMilliField = new Field(
      "d",
      new FieldType(true, new ArrowType.Date(DateUnit.MILLISECOND), null, null),
      java.util.Collections.emptyList())
    val nestedStructField = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(dateMilliField))

    val nestedStructType =
      LanceArrowUtils.fromArrowField(nestedStructField).asInstanceOf[StructType]
    assert(nestedStructType("d").dataType === DateType)

    val keyField = new Field(
      "key",
      new FieldType(false, ArrowType.Utf8.INSTANCE, null, null),
      java.util.Collections.emptyList())
    val valueField = new Field(
      "value",
      new FieldType(true, new ArrowType.Date(DateUnit.MILLISECOND), null, null),
      java.util.Collections.emptyList())
    val entriesField = new Field(
      "entries",
      new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(keyField, valueField))
    val mapField = new Field(
      "m",
      new FieldType(true, new ArrowType.Map(false), null, null),
      java.util.Arrays.asList(entriesField))

    val mapType = LanceArrowUtils.fromArrowField(mapField).asInstanceOf[MapType]
    assert(mapType.keyType === StringType)
    assert(mapType.valueType === DateType)
    assert(mapType.valueContainsNull)
  }

  test("non-microsecond timestamp types") {
    import org.apache.arrow.vector.types.TimeUnit

    // Timestamp with timezone → TimestampType
    for (unit <- Seq(TimeUnit.SECOND, TimeUnit.MILLISECOND, TimeUnit.NANOSECOND)) {
      val field = new Field(
        "ts",
        new FieldType(true, new ArrowType.Timestamp(unit, "UTC"), null, null),
        java.util.Collections.emptyList())
      assert(
        LanceArrowUtils.fromArrowField(field) === TimestampType,
        s"Timestamp($unit, UTC) should map to TimestampType")
    }

    // Timestamp without timezone → TimestampNTZType
    for (unit <- Seq(TimeUnit.SECOND, TimeUnit.MILLISECOND, TimeUnit.NANOSECOND)) {
      val field = new Field(
        "ts",
        new FieldType(true, new ArrowType.Timestamp(unit, null), null, null),
        java.util.Collections.emptyList())
      assert(
        LanceArrowUtils.fromArrowField(field) === TimestampNTZType,
        s"Timestamp($unit, null) should map to TimestampNTZType")
    }
  }

  test("nested non-microsecond timestamp types") {
    import org.apache.arrow.vector.types.TimeUnit

    // Timestamp(SECOND, UTC) inside a struct
    val tsField = new Field(
      "ts",
      new FieldType(true, new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"), null, null),
      java.util.Collections.emptyList())
    val structField = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(tsField))

    val structType =
      LanceArrowUtils.fromArrowField(structField).asInstanceOf[StructType]
    assert(structType("ts").dataType === TimestampType)

    // Timestamp(NANOSECOND, null) as map value
    val keyField = new Field(
      "key",
      new FieldType(false, ArrowType.Utf8.INSTANCE, null, null),
      java.util.Collections.emptyList())
    val valueField = new Field(
      "value",
      new FieldType(true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), null, null),
      java.util.Collections.emptyList())
    val entriesField = new Field(
      "entries",
      new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(keyField, valueField))
    val mapField = new Field(
      "m",
      new FieldType(true, new ArrowType.Map(false), null, null),
      java.util.Arrays.asList(entriesField))

    val mapType = LanceArrowUtils.fromArrowField(mapField).asInstanceOf[MapType]
    assert(mapType.keyType === StringType)
    assert(mapType.valueType === TimestampNTZType)
  }

  test("struct with duplicated field names") {

    def check(dt: DataType, expected: DataType): Unit = {
      val schema = new StructType().add("value", dt)
      intercept[SparkUnsupportedOperationException] {
        LanceArrowUtils.toArrowSchema(schema, null, true)
      }
      assert(LanceArrowUtils.fromArrowSchema(LanceArrowUtils.toArrowSchema(schema, null, false))
        === new StructType().add("value", expected))
    }

    roundtrip(new StructType().add("i", IntegerType).add("i", StringType))

    check(
      new StructType().add("i", IntegerType).add("i", StringType),
      new StructType().add("i_0", IntegerType).add("i_1", StringType))
    check(
      ArrayType(new StructType().add("i", IntegerType).add("i", StringType)),
      ArrayType(new StructType().add("i_0", IntegerType).add("i_1", StringType)))
    check(
      MapType(StringType, new StructType().add("i", IntegerType).add("i", StringType)),
      MapType(StringType, new StructType().add("i_0", IntegerType).add("i_1", StringType)))
  }

  test("large varchar metadata produces LargeUtf8 arrow type") {
    import org.lance.spark.utils.LargeVarCharUtils

    val largeVarCharMetadata = new MetadataBuilder()
      .putString(
        LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_KEY,
        LargeVarCharUtils.ARROW_LARGE_VAR_CHAR_VALUE)
      .build()

    val schema = new StructType()
      .add("regular_string", StringType, nullable = true)
      .add("large_string", StringType, nullable = true, largeVarCharMetadata)

    val arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", false)

    // Regular string should use Utf8
    val regularField = arrowSchema.findField("regular_string")
    assert(regularField.getType === ArrowType.Utf8.INSTANCE)

    // Large string with metadata should use LargeUtf8
    val largeField = arrowSchema.findField("large_string")
    assert(largeField.getType === ArrowType.LargeUtf8.INSTANCE)
  }

  test("Arrow Time types map to TimeType or LongType") {
    import org.apache.arrow.vector.types.TimeUnit

    // All four Arrow Time types should be handled by fromArrowField
    val timeFields = Seq(
      ("time_nano", new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
      ("time_micro", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
      ("time_milli", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
      ("time_sec", new ArrowType.Time(TimeUnit.SECOND, 32)))

    for ((name, arrowTimeType) <- timeFields) {
      val field = new Field(
        name,
        new FieldType(true, arrowTimeType, null, null),
        java.util.Collections.emptyList())
      val sparkType = LanceArrowUtils.fromArrowField(field)
      // On Spark 4.1+ this should be TimeType, on older Spark it should be LongType
      val expectedType = TimeUtils.resolveSparkTimeType()
      assert(
        sparkType === expectedType,
        s"Arrow $arrowTimeType should map to $expectedType, got $sparkType")
    }
  }

  test("nested Arrow Time types") {
    import org.apache.arrow.vector.types.TimeUnit

    // Time field inside a struct
    val timeField = new Field(
      "t",
      new FieldType(true, new ArrowType.Time(TimeUnit.NANOSECOND, 64), null, null),
      java.util.Collections.emptyList())
    val structField = new Field(
      "s",
      new FieldType(true, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(timeField))

    val structType =
      LanceArrowUtils.fromArrowField(structField).asInstanceOf[StructType]
    val expectedType = TimeUtils.resolveSparkTimeType()
    assert(structType("t").dataType === expectedType)
  }

  test("nested Arrow Time types in list and map") {
    import org.apache.arrow.vector.types.TimeUnit

    val expectedType = TimeUtils.resolveSparkTimeType()

    // Time as list element
    val timeElemField = new Field(
      "element",
      new FieldType(true, new ArrowType.Time(TimeUnit.MICROSECOND, 64), null, null),
      java.util.Collections.emptyList())
    val listField = new Field(
      "l",
      new FieldType(true, ArrowType.List.INSTANCE, null, null),
      java.util.Arrays.asList(timeElemField))

    val arrayType = LanceArrowUtils.fromArrowField(listField).asInstanceOf[ArrayType]
    assert(arrayType.elementType === expectedType)

    // Time as map value
    val keyField = new Field(
      "key",
      new FieldType(false, ArrowType.Utf8.INSTANCE, null, null),
      java.util.Collections.emptyList())
    val valueField = new Field(
      "value",
      new FieldType(true, new ArrowType.Time(TimeUnit.SECOND, 32), null, null),
      java.util.Collections.emptyList())
    val entriesField = new Field(
      "entries",
      new FieldType(false, ArrowType.Struct.INSTANCE, null, null),
      java.util.Arrays.asList(keyField, valueField))
    val mapField = new Field(
      "m",
      new FieldType(true, new ArrowType.Map(false), null, null),
      java.util.Arrays.asList(entriesField))

    val mapType = LanceArrowUtils.fromArrowField(mapField).asInstanceOf[MapType]
    assert(mapType.keyType === StringType)
    assert(mapType.valueType === expectedType)
  }

  test("toArrowType for TimeType produces Time(NANOSECOND, 64)") {
    import org.apache.arrow.vector.types.TimeUnit

    if (!TimeUtils.isTimeTypeAvailable) {
      cancel("TimeType not available on this Spark version")
    }
    val timeType = TimeUtils.resolveSparkTimeType()
    val schema = new StructType().add("t", timeType)
    val arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", true)
    val arrowField = arrowSchema.findField("t")
    val arrowTimeType = arrowField.getType.asInstanceOf[ArrowType.Time]
    assert(arrowTimeType.getUnit === TimeUnit.NANOSECOND)
    assert(arrowTimeType.getBitWidth === 64)
  }
}
