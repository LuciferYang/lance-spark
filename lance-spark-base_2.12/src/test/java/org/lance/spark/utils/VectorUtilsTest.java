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

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VectorUtils}.
 *
 * <p>Test inputs use the literal {@value #FSL_KEY} rather than {@link
 * VectorUtils#ARROW_FIXED_SIZE_LIST_SIZE_KEY}, so a rename of the constant will break these tests —
 * catching accidental drift from the wire contract.
 */
public class VectorUtilsTest {

  private static final String FSL_KEY = "arrow.fixed-size-list.size";

  @Test
  public void testIsVectorFieldNullField() {
    assertFalse(VectorUtils.isVectorField(null));
  }

  @Test
  public void testIsVectorFieldNonArrayType() {
    StructField field = DataTypes.createStructField("col", DataTypes.IntegerType, true);
    assertFalse(VectorUtils.isVectorField(field));
  }

  /**
   * Valid metadata on an array-of-String must still return false — guards the element-type filter.
   */
  @Test
  public void testIsVectorFieldArrayOfNonNumeric() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.StringType);
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 128).build();
    StructField field = new StructField("col", arrayType, true, metadata);
    assertFalse(VectorUtils.isVectorField(field));
  }

  /** Integer elements are numeric but not floating-point — must still return false. */
  @Test
  public void testIsVectorFieldArrayOfIntType() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.IntegerType);
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 128).build();
    StructField field = new StructField("col", arrayType, true, metadata);
    assertFalse(VectorUtils.isVectorField(field));
  }

  @Test
  public void testIsVectorFieldFloatArrayNoMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    StructField field = DataTypes.createStructField("col", arrayType, true);
    assertFalse(VectorUtils.isVectorField(field));
  }

  @Test
  public void testIsVectorFieldFloatArrayWithMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 128).build();
    StructField field = new StructField("vec", arrayType, true, metadata);
    assertTrue(VectorUtils.isVectorField(field));
  }

  @Test
  public void testIsVectorFieldDoubleArrayWithMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.DoubleType);
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 64).build();
    StructField field = new StructField("vec", arrayType, true, metadata);
    assertTrue(VectorUtils.isVectorField(field));
  }

  @Test
  public void testGetVectorDimensionNonVectorField() {
    StructField field = DataTypes.createStructField("col", DataTypes.IntegerType, true);
    assertEquals(-1, VectorUtils.getVectorDimension(field));
  }

  /** Key present but stored as String — exercises the try/catch fallback to -1. */
  @Test
  public void testGetVectorDimensionMalformedMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    Metadata metadata = new MetadataBuilder().putString(FSL_KEY, "not-a-number").build();
    StructField field = new StructField("vec", arrayType, true, metadata);
    assertEquals(-1, VectorUtils.getVectorDimension(field));
  }

  @Test
  public void testGetVectorDimensionValidVectorField() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 256).build();
    StructField field = new StructField("vec", arrayType, true, metadata);
    assertEquals(256, VectorUtils.getVectorDimension(field));
  }

  @Test
  public void testIsVectorArrowFieldNullField() {
    assertFalse(VectorUtils.isVectorArrowField(null));
  }

  @Test
  public void testIsVectorArrowFieldNonFixedSizeList() {
    Field field = new Field("col", FieldType.nullable(new ArrowType.Utf8()), null);
    assertFalse(VectorUtils.isVectorArrowField(field));
  }

  @Test
  public void testIsVectorArrowFieldFixedSizeListNoChildren() {
    Field field =
        new Field(
            "vec", FieldType.nullable(new ArrowType.FixedSizeList(128)), Collections.emptyList());
    assertFalse(VectorUtils.isVectorArrowField(field));
  }

  /** Int child on a FixedSizeList must return false — guards the floating-point filter. */
  @Test
  public void testIsVectorArrowFieldFixedSizeListNonFloatChild() {
    Field child = new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field field =
        new Field(
            "vec",
            FieldType.nullable(new ArrowType.FixedSizeList(128)),
            Collections.singletonList(child));
    assertFalse(VectorUtils.isVectorArrowField(field));
  }

  @Test
  public void testIsVectorArrowFieldValidFloatVector() {
    Field child =
        new Field(
            "item",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null);
    Field field =
        new Field(
            "vec",
            FieldType.nullable(new ArrowType.FixedSizeList(128)),
            Collections.singletonList(child));
    assertTrue(VectorUtils.isVectorArrowField(field));
  }

  @Test
  public void testIsVectorArrowFieldValidDoubleVector() {
    Field child =
        new Field(
            "item",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null);
    Field field =
        new Field(
            "vec",
            FieldType.nullable(new ArrowType.FixedSizeList(64)),
            Collections.singletonList(child));
    assertTrue(VectorUtils.isVectorArrowField(field));
  }

  @Test
  public void testGetVectorArrowDimensionNonVectorField() {
    Field field = new Field("col", FieldType.nullable(new ArrowType.Utf8()), null);
    assertEquals(-1, VectorUtils.getVectorArrowDimension(field));
  }

  @Test
  public void testGetVectorArrowDimensionValidVector() {
    Field child =
        new Field(
            "item",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null);
    Field field =
        new Field(
            "vec",
            FieldType.nullable(new ArrowType.FixedSizeList(256)),
            Collections.singletonList(child));
    assertEquals(256, VectorUtils.getVectorArrowDimension(field));
  }

  @Test
  public void testShouldBeFixedSizeListNullMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    assertFalse(VectorUtils.shouldBeFixedSizeList(arrayType, null));
  }

  @Test
  public void testShouldBeFixedSizeListNoMetadataKey() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    assertFalse(VectorUtils.shouldBeFixedSizeList(arrayType, Metadata.empty()));
  }

  @Test
  public void testShouldBeFixedSizeListNonArrayType() {
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 128).build();
    assertFalse(VectorUtils.shouldBeFixedSizeList(DataTypes.StringType, metadata));
  }

  @Test
  public void testShouldBeFixedSizeListArrayOfNonNumeric() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.StringType);
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 128).build();
    assertFalse(VectorUtils.shouldBeFixedSizeList(arrayType, metadata));
  }

  @Test
  public void testShouldBeFixedSizeListValidFloat() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 128).build();
    assertTrue(VectorUtils.shouldBeFixedSizeList(arrayType, metadata));
  }

  @Test
  public void testShouldBeFixedSizeListValidDouble() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.DoubleType);
    Metadata metadata = new MetadataBuilder().putLong(FSL_KEY, 64).build();
    assertTrue(VectorUtils.shouldBeFixedSizeList(arrayType, metadata));
  }

  @Test
  public void testCreateVectorSizePropertyKey() {
    assertEquals(
        "embeddings.arrow.fixed-size-list.size",
        VectorUtils.createVectorSizePropertyKey("embeddings"));
  }
}
