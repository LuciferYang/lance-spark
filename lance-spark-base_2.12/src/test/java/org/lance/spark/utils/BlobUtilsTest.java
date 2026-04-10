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

/** Unit tests for {@link BlobUtils}. */
public class BlobUtilsTest {

  @Test
  public void testIsBlobSparkField_nullField() {
    assertFalse(BlobUtils.isBlobSparkField(null));
  }

  @Test
  public void testIsBlobSparkField_emptyMetadata() {
    StructField field = DataTypes.createStructField("col", DataTypes.BinaryType, true);
    assertFalse(BlobUtils.isBlobSparkField(field));
  }

  @Test
  public void testIsBlobSparkField_noKey() {
    Metadata metadata = new MetadataBuilder().putString("other-key", "value").build();
    StructField field = new StructField("col", DataTypes.BinaryType, true, metadata);
    assertFalse(BlobUtils.isBlobSparkField(field));
  }

  @Test
  public void testIsBlobSparkField_wrongValue() {
    Metadata metadata =
        new MetadataBuilder().putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, "false").build();
    StructField field = new StructField("col", DataTypes.BinaryType, true, metadata);
    assertFalse(BlobUtils.isBlobSparkField(field));
  }

  @Test
  public void testIsBlobSparkField_valid() {
    Metadata metadata =
        new MetadataBuilder()
            .putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE)
            .build();
    StructField field = new StructField("col", DataTypes.BinaryType, true, metadata);
    assertTrue(BlobUtils.isBlobSparkField(field));
  }

  @Test
  public void testIsBlobSparkField_caseInsensitive() {
    Metadata metadata =
        new MetadataBuilder().putString(BlobUtils.LANCE_ENCODING_BLOB_KEY, "TRUE").build();
    StructField field = new StructField("col", DataTypes.BinaryType, true, metadata);
    assertTrue(BlobUtils.isBlobSparkField(field));
  }

  @Test
  public void testIsBlobArrowField_nullField() {
    assertFalse(BlobUtils.isBlobArrowField(null));
  }

  @Test
  public void testIsBlobArrowField_noMetadata() {
    Field field = new Field("col", FieldType.nullable(new ArrowType.Binary()), null);
    assertFalse(BlobUtils.isBlobArrowField(field));
  }

  @Test
  public void testIsBlobArrowField_noKey() {
    Map<String, String> meta = new HashMap<>();
    meta.put("other-key", "value");
    Field field = new Field("col", new FieldType(true, new ArrowType.Binary(), null, meta), null);
    assertFalse(BlobUtils.isBlobArrowField(field));
  }

  @Test
  public void testIsBlobArrowField_wrongValue() {
    Map<String, String> meta = new HashMap<>();
    meta.put(BlobUtils.LANCE_ENCODING_BLOB_KEY, "false");
    Field field = new Field("col", new FieldType(true, new ArrowType.Binary(), null, meta), null);
    assertFalse(BlobUtils.isBlobArrowField(field));
  }

  @Test
  public void testIsBlobArrowField_valid() {
    Map<String, String> meta = new HashMap<>();
    meta.put(BlobUtils.LANCE_ENCODING_BLOB_KEY, BlobUtils.LANCE_ENCODING_BLOB_VALUE);
    Field field = new Field("col", new FieldType(true, new ArrowType.Binary(), null, meta), null);
    assertTrue(BlobUtils.isBlobArrowField(field));
  }

  @Test
  public void testIsBlobArrowField_caseInsensitive() {
    Map<String, String> meta = new HashMap<>();
    meta.put(BlobUtils.LANCE_ENCODING_BLOB_KEY, "True");
    Field field = new Field("col", new FieldType(true, new ArrowType.Binary(), null, meta), null);
    assertTrue(BlobUtils.isBlobArrowField(field));
  }
}
