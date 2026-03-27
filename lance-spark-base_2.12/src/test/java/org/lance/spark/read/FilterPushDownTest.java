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

import org.lance.spark.utils.Optional;

import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

public class FilterPushDownTest {
  @Test
  public void testCompileFiltersToSqlWhereClause() {
    // Test case 1: GreaterThan, LessThanOrEqual, IsNotNull
    Filter[] filters1 =
        new Filter[] {
          new GreaterThan("age", 30), new LessThanOrEqual("salary", 100000), new IsNotNull("name")
        };
    Optional<String> whereClause1 = FilterPushDown.compileFiltersToSqlWhereClause(filters1, null);
    assertTrue(whereClause1.isPresent());
    assertEquals("(age > 30) AND (salary <= 100000) AND (name IS NOT NULL)", whereClause1.get());

    // Test case 2: GreaterThan, StringContains, LessThan
    Filter[] filters2 =
        new Filter[] {
          new GreaterThan("age", 30),
          new StringContains("name", "John"),
          new LessThan("salary", 50000)
        };
    Optional<String> whereClause2 = FilterPushDown.compileFiltersToSqlWhereClause(filters2, null);
    assertTrue(whereClause2.isPresent());
    assertEquals("(age > 30) AND (salary < 50000)", whereClause2.get());

    // Test case 3: Empty filters array
    Filter[] filters3 = new Filter[] {};
    Optional<String> whereClause3 = FilterPushDown.compileFiltersToSqlWhereClause(filters3, null);
    assertFalse(whereClause3.isPresent());

    // Test case 4: Mixed supported and unsupported filters
    Filter[] filters4 =
        new Filter[] {
          new GreaterThan("age", 30),
          new StringContains("name", "John"),
          new IsNull("address"),
          new EqualTo("country", "USA")
        };
    Optional<String> whereClause4 = FilterPushDown.compileFiltersToSqlWhereClause(filters4, null);
    assertTrue(whereClause4.isPresent());
    assertEquals("(age > 30) AND (address IS NULL) AND (country == 'USA')", whereClause4.get());

    // Test case 5: Not, Or, And combinations
    Filter[] filters5 =
        new Filter[] {
          new Not(new GreaterThan("age", 30)),
          new Or(new IsNotNull("name"), new IsNull("address")),
          new And(new LessThan("salary", 100000), new GreaterThanOrEqual("salary", 50000))
        };
    Optional<String> whereClause5 = FilterPushDown.compileFiltersToSqlWhereClause(filters5, null);
    assertTrue(whereClause5.isPresent());
    assertEquals(
        "(NOT (age > 30)) AND ((name IS NOT NULL) OR (address IS NULL)) AND ((salary < 100000) AND (salary >= 50000))",
        whereClause5.get());
  }

  @Test
  public void testCompileFiltersToSqlWhereClauseWithEmptyFilters() {
    Filter[] filters = new Filter[] {};

    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertFalse(whereClause.isPresent());
  }

  @Test
  public void testIntegerInFilterPushDown() {
    Object[] values = new Object[2];
    values[0] = 500;
    values[1] = 600;
    Filter[] filters = new Filter[] {new GreaterThan("age", 30), new In("salary", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertTrue(whereClause.isPresent());
    assertEquals("(age > 30) AND (salary IN (500,600))", whereClause.get());
  }

  @Test
  public void testStringInFilterPushDown() {
    Object[] values = new Object[2];
    values[0] = "500";
    values[1] = "600";
    Filter[] filters = new Filter[] {new GreaterThan("age", 30), new In("salary", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertTrue(whereClause.isPresent());
    assertEquals("(age > 30) AND (salary IN ('500','600'))", whereClause.get());
  }

  @Test
  public void testDecimalFilterPushDown() {
    // Decimal comparisons must use CAST so Lance's DataFusion parser produces Decimal128,
    // not Float64, which would fail type resolution against Decimal columns.
    Filter[] filters =
        new Filter[] {
          new GreaterThanOrEqual("net_profit", new BigDecimal("100.00")),
          new LessThanOrEqual("net_profit", new BigDecimal("200.00"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(net_profit >= CAST(100.00 AS DECIMAL(5, 2))) AND (net_profit <= CAST(200.00 AS DECIMAL(5, 2)))",
        whereClause.get());
  }

  @Test
  public void testDecimalInFilterPushDown() {
    Object[] values =
        new Object[] {new BigDecimal("100.00"), new BigDecimal("150.00"), new BigDecimal("200.00")};
    Filter[] filters = new Filter[] {new In("price", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(price IN (CAST(100.00 AS DECIMAL(5, 2)),CAST(150.00 AS DECIMAL(5, 2)),CAST(200.00 AS DECIMAL(5, 2))))",
        whereClause.get());
  }

  @Test
  public void testDecimalWithVaryingScaleAndPrecision() {
    // Verify precision/scale are taken from the BigDecimal value itself
    Filter[] filters =
        new Filter[] {
          new GreaterThan("amount", new BigDecimal("1234567.89")),
          new LessThan("amount", new BigDecimal("0.5"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(amount > CAST(1234567.89 AS DECIMAL(9, 2))) AND (amount < CAST(0.5 AS DECIMAL(1, 1)))",
        whereClause.get());
  }

  @Test
  public void testDecimalZeroValuePrecisionClamped() {
    // Java's BigDecimal returns precision=1 for zero regardless of scale, e.g.
    // new BigDecimal("0.00") has precision=1 and scale=2. Arrow rejects DECIMAL(1,2) because
    // scale > precision is invalid. The fix clamps: precision = max(precision, scale).
    Filter[] filters =
        new Filter[] {
          new GreaterThan("net_paid", new BigDecimal("0.00")),
          new GreaterThan("net_profit", new BigDecimal("1.00"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(net_paid > CAST(0.00 AS DECIMAL(2, 2))) AND (net_profit > CAST(1.00 AS DECIMAL(3, 2)))",
        whereClause.get());
  }

  @Test
  public void testDateFilterPushDown() {
    // Date literals must use the 'date' keyword so Lance's DataFusion parser produces Date32,
    // not Utf8, which would fail type resolution against Date columns.
    Filter[] filters =
        new Filter[] {
          new GreaterThanOrEqual("d_date", Date.valueOf("2000-08-23")),
          new LessThanOrEqual("d_date", Date.valueOf("2000-09-06"))
        };
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(d_date >= date '2000-08-23') AND (d_date <= date '2000-09-06')", whereClause.get());
  }

  @Test
  public void testTimestampFilterPushDown() {
    // Timestamp literals must use the 'timestamp' keyword so Lance's DataFusion parser produces
    // Timestamp, not Utf8.
    Filter[] filters =
        new Filter[] {new EqualTo("created_at", Timestamp.valueOf("2024-01-15 10:30:00.0"))};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, null);
    assertTrue(whereClause.isPresent());
    assertEquals("(created_at == timestamp '2024-01-15 10:30:00.0')", whereClause.get());
  }

  @Test
  public void testBigDecimalUsesColumnType() {
    // When schema is available, BigDecimal should use the column's precision/scale
    // rather than the value's own precision/scale, for consistency.
    StructType schema = new StructType().add("net_profit", DataTypes.createDecimalType(7, 2));
    Filter[] filters =
        new Filter[] {new GreaterThanOrEqual("net_profit", new BigDecimal("100.00"))};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, schema);
    assertTrue(whereClause.isPresent());
    assertEquals("(net_profit >= CAST(100.00 AS DECIMAL(7, 2)))", whereClause.get());
  }

  @Test
  public void testDoubleToDecimalCast() {
    // Spark sometimes pushes Double literals for Decimal columns (e.g. TPC-DS q32).
    // Without schema-aware casting, the bare "0.99" is parsed as Float64 by DataFusion,
    // which rejects Float64-to-Decimal128 conversion.
    StructType schema = new StructType().add("i_current_price", DataTypes.createDecimalType(7, 2));
    Filter[] filters = new Filter[] {new GreaterThanOrEqual("i_current_price", 0.99)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, schema);
    assertTrue(whereClause.isPresent());
    assertEquals("(i_current_price >= CAST(0.99 AS DECIMAL(7, 2)))", whereClause.get());
  }

  @Test
  public void testIntegerToDecimalCast() {
    // Spark may push Integer literals for Decimal columns.
    StructType schema = new StructType().add("price", DataTypes.createDecimalType(10, 2));
    Filter[] filters = new Filter[] {new EqualTo("price", 100)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, schema);
    assertTrue(whereClause.isPresent());
    assertEquals("(price == CAST(100 AS DECIMAL(10, 2)))", whereClause.get());
  }

  @Test
  public void testLongToDecimalCast() {
    StructType schema = new StructType().add("amount", DataTypes.createDecimalType(18, 0));
    Filter[] filters = new Filter[] {new LessThan("amount", 999999999L)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, schema);
    assertTrue(whereClause.isPresent());
    assertEquals("(amount < CAST(999999999 AS DECIMAL(18, 0)))", whereClause.get());
  }

  @Test
  public void testStringToDateCast() {
    // Spark sometimes pushes String literals for Date columns (e.g. TPC-DS q58).
    // Without schema-aware casting, the string '2000-01-27' is parsed as Utf8 by DataFusion,
    // which rejects Utf8-to-Date32 conversion.
    StructType schema = new StructType().add("d_date", DataTypes.DateType);
    Filter[] filters = new Filter[] {new GreaterThanOrEqual("d_date", "2000-01-27")};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, schema);
    assertTrue(whereClause.isPresent());
    assertEquals("(d_date >= date '2000-01-27')", whereClause.get());
  }

  @Test
  public void testStringToTimestampCast() {
    StructType schema = new StructType().add("event_time", DataTypes.TimestampType);
    Filter[] filters = new Filter[] {new LessThan("event_time", "2024-01-15 10:30:00")};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, schema);
    assertTrue(whereClause.isPresent());
    assertEquals("(event_time < timestamp '2024-01-15 10:30:00')", whereClause.get());
  }

  @Test
  public void testDoubleInDecimalColumn() {
    // IN clause with Double values for a Decimal column.
    StructType schema = new StructType().add("price", DataTypes.createDecimalType(7, 2));
    Object[] values = new Object[] {1.5, 2.5, 3.5};
    Filter[] filters = new Filter[] {new In("price", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, schema);
    assertTrue(whereClause.isPresent());
    assertEquals(
        "(price IN (CAST(1.5 AS DECIMAL(7, 2)),CAST(2.5 AS DECIMAL(7, 2)),CAST(3.5 AS DECIMAL(7, 2))))",
        whereClause.get());
  }

  @Test
  public void testStringInDateColumn() {
    // IN clause with String values for a Date column.
    StructType schema = new StructType().add("d_date", DataTypes.DateType);
    Object[] values = new Object[] {"2000-01-27", "2000-02-15"};
    Filter[] filters = new Filter[] {new In("d_date", values)};
    Optional<String> whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters, schema);
    assertTrue(whereClause.isPresent());
    assertEquals("(d_date IN (date '2000-01-27',date '2000-02-15'))", whereClause.get());
  }
}
