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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LargeVarCharUtils}.
 *
 * <p>Test inputs use the literal key/value strings rather than the exposed constants, so a rename
 * of {@code ARROW_LARGE_VAR_CHAR_KEY} / {@code ARROW_LARGE_VAR_CHAR_VALUE} will break these tests.
 * End-to-end validation against real lance data lives in {@link
 * org.lance.spark.utils.SchemaConverterTest} and the LanceArrow suites.
 */
public class LargeVarCharUtilsTest {

  private static final String LVC_KEY = "arrow:large-var-char";

  @Test
  public void testIsLargeVarCharSparkFieldNullField() {
    assertFalse(LargeVarCharUtils.isLargeVarCharSparkField(null));
  }

  /** Valid metadata on a non-String column must still return false — guards the type filter. */
  @Test
  public void testIsLargeVarCharSparkFieldNonStringType() {
    Metadata metadata = new MetadataBuilder().putString(LVC_KEY, "true").build();
    StructField field = new StructField("col", DataTypes.IntegerType, true, metadata);
    assertFalse(LargeVarCharUtils.isLargeVarCharSparkField(field));
  }

  @Test
  public void testIsLargeVarCharSparkFieldStringNoMetadata() {
    StructField field = DataTypes.createStructField("col", DataTypes.StringType, true);
    assertFalse(LargeVarCharUtils.isLargeVarCharSparkField(field));
  }

  @Test
  public void testIsLargeVarCharSparkFieldStringNoKey() {
    Metadata metadata = new MetadataBuilder().putString("other-key", "value").build();
    StructField field = new StructField("col", DataTypes.StringType, true, metadata);
    assertFalse(LargeVarCharUtils.isLargeVarCharSparkField(field));
  }

  @Test
  public void testIsLargeVarCharSparkFieldWrongValue() {
    Metadata metadata = new MetadataBuilder().putString(LVC_KEY, "false").build();
    StructField field = new StructField("col", DataTypes.StringType, true, metadata);
    assertFalse(LargeVarCharUtils.isLargeVarCharSparkField(field));
  }

  @Test
  public void testIsLargeVarCharSparkFieldCaseInsensitive() {
    Metadata metadata = new MetadataBuilder().putString(LVC_KEY, "TRUE").build();
    StructField field = new StructField("col", DataTypes.StringType, true, metadata);
    assertTrue(LargeVarCharUtils.isLargeVarCharSparkField(field));
  }

  @Test
  public void testIsLargeVarCharArrowFieldNullField() {
    assertFalse(LargeVarCharUtils.isLargeVarCharArrowField(null));
  }

  @Test
  public void testIsLargeVarCharArrowFieldNoMetadata() {
    Field field = new Field("col", FieldType.nullable(new ArrowType.Utf8()), null);
    assertFalse(LargeVarCharUtils.isLargeVarCharArrowField(field));
  }

  @Test
  public void testIsLargeVarCharArrowFieldNoKey() {
    Map<String, String> meta = new HashMap<>();
    meta.put("other-key", "value");
    Field field = new Field("col", new FieldType(true, new ArrowType.Utf8(), null, meta), null);
    assertFalse(LargeVarCharUtils.isLargeVarCharArrowField(field));
  }

  @Test
  public void testIsLargeVarCharArrowFieldWrongValue() {
    Map<String, String> meta = new HashMap<>();
    meta.put(LVC_KEY, "false");
    Field field = new Field("col", new FieldType(true, new ArrowType.Utf8(), null, meta), null);
    assertFalse(LargeVarCharUtils.isLargeVarCharArrowField(field));
  }

  @Test
  public void testIsLargeVarCharArrowFieldCaseInsensitive() {
    Map<String, String> meta = new HashMap<>();
    meta.put(LVC_KEY, "True");
    Field field = new Field("col", new FieldType(true, new ArrowType.Utf8(), null, meta), null);
    assertTrue(LargeVarCharUtils.isLargeVarCharArrowField(field));
  }

  @Test
  public void testCreatePropertyKey() {
    assertEquals(
        "my_column.arrow.large_var_char", LargeVarCharUtils.createPropertyKey("my_column"));
  }
}
