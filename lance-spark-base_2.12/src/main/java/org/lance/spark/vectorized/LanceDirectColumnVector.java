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
package org.lance.spark.vectorized;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Zero-copy ColumnVector that reads directly from Rust-decoded buffer addresses using Unsafe memory
 * access. No Arrow Java objects involved.
 */
public class LanceDirectColumnVector extends ColumnVector {
  private final long dataAddress;
  private final long validityAddress;
  private final int nullCount;
  private final int typeWidth;

  public LanceDirectColumnVector(
      DataType type, long dataAddress, long validityAddress, int nullCount, int typeWidth) {
    super(type);
    this.dataAddress = dataAddress;
    this.validityAddress = validityAddress;
    this.nullCount = nullCount;
    this.typeWidth = typeWidth;
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNull() {
    return nullCount > 0;
  }

  @Override
  public int numNulls() {
    return nullCount;
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (nullCount == 0) return false;
    long byteAddr = validityAddress + (rowId >> 3);
    byte b = Platform.getByte(null, byteAddr);
    return (b & (1 << (rowId & 7))) == 0;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return Platform.getByte(null, dataAddress + rowId) != 0;
  }

  @Override
  public byte getByte(int rowId) {
    return Platform.getByte(null, dataAddress + rowId);
  }

  @Override
  public short getShort(int rowId) {
    return Platform.getShort(null, dataAddress + (long) rowId * 2);
  }

  @Override
  public int getInt(int rowId) {
    return Platform.getInt(null, dataAddress + (long) rowId * 4);
  }

  @Override
  public long getLong(int rowId) {
    return Platform.getLong(null, dataAddress + (long) rowId * 8);
  }

  @Override
  public float getFloat(int rowId) {
    return Platform.getFloat(null, dataAddress + (long) rowId * 4);
  }

  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(null, dataAddress + (long) rowId * 8);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    if (precision <= 18) {
      long low = Platform.getLong(null, dataAddress + (long) rowId * 16);
      return Decimal.apply(low, precision, scale);
    }
    return null;
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException("String not supported in direct reader");
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("Binary not supported in direct reader");
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("Array not supported in direct reader");
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException("Map not supported in direct reader");
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("Child not supported in direct reader");
  }
}
