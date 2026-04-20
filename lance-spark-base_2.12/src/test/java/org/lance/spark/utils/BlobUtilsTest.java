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
 * Unit tests for {@link BlobUtils}.
 *
 * <p>Test inputs use the literal key/value strings rather than the constants exposed by {@link
 * BlobUtils}, so a rename of {@code LANCE_ENCODING_BLOB_KEY} or {@code LANCE_ENCODING_BLOB_VALUE}
 * will break these tests — catching accidental drift from the wire contract.
 *
 * <p>Upstream convention changes (lance-core itself switching to a different key) are an
 * integration-level concern covered by {@link org.lance.spark.BaseBlobCreateTableTest} and {@link
 * org.lance.spark.utils.SchemaConverterTest}, which round-trip through real data.
 */
public class BlobUtilsTest {

  private static final String BLOB_KEY = "lance-encoding:blob";

  @Test
  public void testIsBlobSparkFieldNullField() {
    assertFalse(BlobUtils.isBlobSparkField(null));
  }

  @Test
  public void testIsBlobSparkFieldEmptyMetadata() {
    StructField field = DataTypes.createStructField("col", DataTypes.BinaryType, true);
    assertFalse(BlobUtils.isBlobSparkField(field));
  }

  @Test
  public void testIsBlobSparkFieldNoKey() {
    Metadata metadata = new MetadataBuilder().putString("other-key", "value").build();
    StructField field = new StructField("col", DataTypes.BinaryType, true, metadata);
    assertFalse(BlobUtils.isBlobSparkField(field));
  }

  @Test
  public void testIsBlobSparkFieldWrongValue() {
    Metadata metadata = new MetadataBuilder().putString(BLOB_KEY, "false").build();
    StructField field = new StructField("col", DataTypes.BinaryType, true, metadata);
    assertFalse(BlobUtils.isBlobSparkField(field));
  }

  /**
   * Positive match via mixed case — guards against the matcher being tightened to {@code .equals}.
   */
  @Test
  public void testIsBlobSparkFieldCaseInsensitive() {
    Metadata metadata = new MetadataBuilder().putString(BLOB_KEY, "TRUE").build();
    StructField field = new StructField("col", DataTypes.BinaryType, true, metadata);
    assertTrue(BlobUtils.isBlobSparkField(field));
  }

  @Test
  public void testIsBlobArrowFieldNullField() {
    assertFalse(BlobUtils.isBlobArrowField(null));
  }

  @Test
  public void testIsBlobArrowFieldNoMetadata() {
    Field field = new Field("col", FieldType.nullable(new ArrowType.Binary()), null);
    assertFalse(BlobUtils.isBlobArrowField(field));
  }

  @Test
  public void testIsBlobArrowFieldNoKey() {
    Map<String, String> meta = new HashMap<>();
    meta.put("other-key", "value");
    Field field = new Field("col", new FieldType(true, new ArrowType.Binary(), null, meta), null);
    assertFalse(BlobUtils.isBlobArrowField(field));
  }

  @Test
  public void testIsBlobArrowFieldWrongValue() {
    Map<String, String> meta = new HashMap<>();
    meta.put(BLOB_KEY, "false");
    Field field = new Field("col", new FieldType(true, new ArrowType.Binary(), null, meta), null);
    assertFalse(BlobUtils.isBlobArrowField(field));
  }

  @Test
  public void testIsBlobArrowFieldCaseInsensitive() {
    Map<String, String> meta = new HashMap<>();
    meta.put(BLOB_KEY, "True");
    Field field = new Field("col", new FieldType(true, new ArrowType.Binary(), null, meta), null);
    assertTrue(BlobUtils.isBlobArrowField(field));
  }
}
