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

/** Unit tests for {@link VectorUtils}. */
public class VectorUtilsTest {

  @Test
  public void testIsVectorField_nullField() {
    assertFalse(VectorUtils.isVectorField(null));
  }

  @Test
  public void testIsVectorField_nonArrayType() {
    StructField field = DataTypes.createStructField("col", DataTypes.IntegerType, true);
    assertFalse(VectorUtils.isVectorField(field));
  }

  @Test
  public void testIsVectorField_arrayOfNonNumeric() {
    // Provide valid vector metadata so the element-type check (not the metadata-absent check)
    // is the branch that returns false
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.StringType);
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 128).build();
    StructField field = new StructField("col", arrayType, true, metadata);
    assertFalse(VectorUtils.isVectorField(field));
  }

  @Test
  public void testIsVectorField_arrayOfIntType() {
    // Provide valid vector metadata so the element-type check (not the metadata-absent check)
    // is the branch that returns false
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.IntegerType);
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 128).build();
    StructField field = new StructField("col", arrayType, true, metadata);
    assertFalse(VectorUtils.isVectorField(field));
  }

  @Test
  public void testIsVectorField_floatArrayNoMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    StructField field = DataTypes.createStructField("col", arrayType, true);
    assertFalse(VectorUtils.isVectorField(field));
  }

  @Test
  public void testIsVectorField_floatArrayWithMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 128).build();
    StructField field = new StructField("vec", arrayType, true, metadata);
    assertTrue(VectorUtils.isVectorField(field));
  }

  @Test
  public void testIsVectorField_doubleArrayWithMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.DoubleType);
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 64).build();
    StructField field = new StructField("vec", arrayType, true, metadata);
    assertTrue(VectorUtils.isVectorField(field));
  }

  @Test
  public void testGetVectorDimension_nonVectorField() {
    StructField field = DataTypes.createStructField("col", DataTypes.IntegerType, true);
    assertEquals(-1, VectorUtils.getVectorDimension(field));
  }

  @Test
  public void testGetVectorDimension_malformedMetadata() {
    // Metadata key exists but is not a long → exercises the try-catch fallback to -1
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    Metadata metadata =
        new MetadataBuilder()
            .putString(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, "not-a-number")
            .build();
    StructField field = new StructField("vec", arrayType, true, metadata);
    assertEquals(-1, VectorUtils.getVectorDimension(field));
  }

  @Test
  public void testGetVectorDimension_validVectorField() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 256).build();
    StructField field = new StructField("vec", arrayType, true, metadata);
    assertEquals(256, VectorUtils.getVectorDimension(field));
  }

  @Test
  public void testIsVectorArrowField_nullField() {
    assertFalse(VectorUtils.isVectorArrowField(null));
  }

  @Test
  public void testIsVectorArrowField_nonFixedSizeList() {
    Field field = new Field("col", FieldType.nullable(new ArrowType.Utf8()), null);
    assertFalse(VectorUtils.isVectorArrowField(field));
  }

  @Test
  public void testIsVectorArrowField_fixedSizeListNoChildren() {
    Field field =
        new Field(
            "vec", FieldType.nullable(new ArrowType.FixedSizeList(128)), Collections.emptyList());
    assertFalse(VectorUtils.isVectorArrowField(field));
  }

  @Test
  public void testIsVectorArrowField_fixedSizeListNonFloatChild() {
    Field child = new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field field =
        new Field(
            "vec",
            FieldType.nullable(new ArrowType.FixedSizeList(128)),
            Collections.singletonList(child));
    assertFalse(VectorUtils.isVectorArrowField(field));
  }

  @Test
  public void testIsVectorArrowField_validFloatVector() {
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
  public void testIsVectorArrowField_validDoubleVector() {
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
  public void testGetVectorArrowDimension_nonVectorField() {
    Field field = new Field("col", FieldType.nullable(new ArrowType.Utf8()), null);
    assertEquals(-1, VectorUtils.getVectorArrowDimension(field));
  }

  @Test
  public void testGetVectorArrowDimension_validVector() {
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
  public void testShouldBeFixedSizeList_nullMetadata() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    assertFalse(VectorUtils.shouldBeFixedSizeList(arrayType, null));
  }

  @Test
  public void testShouldBeFixedSizeList_noMetadataKey() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    assertFalse(VectorUtils.shouldBeFixedSizeList(arrayType, Metadata.empty()));
  }

  @Test
  public void testShouldBeFixedSizeList_nonArrayType() {
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 128).build();
    assertFalse(VectorUtils.shouldBeFixedSizeList(DataTypes.StringType, metadata));
  }

  @Test
  public void testShouldBeFixedSizeList_arrayOfNonNumeric() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.StringType);
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 128).build();
    assertFalse(VectorUtils.shouldBeFixedSizeList(arrayType, metadata));
  }

  @Test
  public void testShouldBeFixedSizeList_validFloat() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.FloatType);
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 128).build();
    assertTrue(VectorUtils.shouldBeFixedSizeList(arrayType, metadata));
  }

  @Test
  public void testShouldBeFixedSizeList_validDouble() {
    ArrayType arrayType = DataTypes.createArrayType(DataTypes.DoubleType);
    Metadata metadata =
        new MetadataBuilder().putLong(VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY, 64).build();
    assertTrue(VectorUtils.shouldBeFixedSizeList(arrayType, metadata));
  }

  @Test
  public void testCreateVectorSizePropertyKey() {
    assertEquals(
        "embeddings." + VectorUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY,
        VectorUtils.createVectorSizePropertyKey("embeddings"));
  }
}
