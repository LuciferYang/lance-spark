/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.lance.spark.native_reader;

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

/**
 * Decodes a miniblock chunk and writes values to an OnHeapColumnVector.
 * Supports int32, int64, float, double, and decimal128 (precision <= 18).
 */
public class LanceMiniBlockDecoder {
  private final int[] intBuffer = new int[1024];
  private final long[] longBuffer = new long[1024];
  private final boolean[] validityBuffer = new boolean[1024];

  /**
   * Decode one miniblock chunk and write to the column vector.
   *
   * @param chunkData raw chunk data buffer
   * @param dataOffset offset to the start of this chunk in chunkData
   * @param numValues number of values in this chunk
   * @param col output column vector
   * @param colOffset row offset in the column vector
   * @param dataType Spark data type
   * @param typeWidth bytes per value in the raw data (4 for int32/float, 8 for int64/double, 16 for decimal128)
   */
  public void decodeChunk(byte[] chunkData, int dataOffset, int numValues,
      OnHeapColumnVector col, int colOffset, DataType dataType, int typeWidth) {
    // Parse chunk header: [num_levels: u16][def_size: u16][value_buf_size: u32]
    int defSize = (chunkData[dataOffset + 2] & 0xFF)
        | ((chunkData[dataOffset + 3] & 0xFF) << 8);

    // Decode validity
    LanceRleDecoder.decodeValidity(chunkData, dataOffset + 8, defSize,
        numValues, validityBuffer, 0);

    // Skip to value data (after header + def data + padding)
    int offset = dataOffset + 8 + defSize;
    offset = (offset + 7) & ~7; // align to 8

    // Read bit_width
    int bitWidth;
    if (typeWidth <= 4) {
      bitWidth = (chunkData[offset] & 0xFF) | ((chunkData[offset + 1] & 0xFF) << 8)
          | ((chunkData[offset + 2] & 0xFF) << 16) | ((chunkData[offset + 3] & 0xFF) << 24);
      offset += 4;
    } else {
      // For u64/u128: bit_width header is typeWidth bytes, read as long
      bitWidth = (int) getLongLE(chunkData, offset);
      offset += typeWidth;
    }

    // Decode and write based on type
    if (dataType == DataTypes.IntegerType) {
      FastLanesBitpacking.unpack1024(chunkData, offset, bitWidth, intBuffer, 0);
      for (int i = 0; i < numValues; i++) {
        if (validityBuffer[i]) {
          col.putInt(colOffset + i, intBuffer[i]);
        } else {
          col.putNull(colOffset + i);
        }
      }
    } else if (dataType == DataTypes.LongType) {
      FastLanesBitpacking.unpack1024Long(chunkData, offset, bitWidth, longBuffer, 0);
      for (int i = 0; i < numValues; i++) {
        if (validityBuffer[i]) {
          col.putLong(colOffset + i, longBuffer[i]);
        } else {
          col.putNull(colOffset + i);
        }
      }
    } else if (dataType == DataTypes.FloatType) {
      // Float is stored as bitpacked u32, reinterpret as float
      FastLanesBitpacking.unpack1024(chunkData, offset, bitWidth, intBuffer, 0);
      for (int i = 0; i < numValues; i++) {
        if (validityBuffer[i]) {
          col.putFloat(colOffset + i, Float.intBitsToFloat(intBuffer[i]));
        } else {
          col.putNull(colOffset + i);
        }
      }
    } else if (dataType == DataTypes.DoubleType) {
      // Double is stored as bitpacked u64, reinterpret as double
      FastLanesBitpacking.unpack1024Long(chunkData, offset, bitWidth, longBuffer, 0);
      for (int i = 0; i < numValues; i++) {
        if (validityBuffer[i]) {
          col.putDouble(colOffset + i, Double.longBitsToDouble(longBuffer[i]));
        } else {
          col.putNull(colOffset + i);
        }
      }
    } else if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      if (dt.precision() <= 18) {
        // Decimal128 stored as bitpacked u128, but for precision<=18 the low 64 bits suffice
        // The value data uses 128-bit (16 bytes) per element
        // For bitpacked decimal: bit_width applies to the full 128-bit value
        // MVP: read low 8 bytes of each 16-byte value as the unscaled long
        // This works when values fit in 64 bits (precision <= 18)
        for (int i = 0; i < numValues; i++) {
          if (validityBuffer[i]) {
            long unscaled = getLongLE(chunkData, offset + i * 16);
            col.putLong(colOffset + i, unscaled);
          } else {
            col.putNull(colOffset + i);
          }
        }
      }
    }
  }

  /**
   * Get the type width in bytes for a given data type.
   */
  public static int getTypeWidth(DataType dataType) {
    if (dataType == DataTypes.IntegerType || dataType == DataTypes.FloatType) return 4;
    if (dataType == DataTypes.LongType || dataType == DataTypes.DoubleType) return 8;
    if (dataType instanceof DecimalType) return 16;
    return -1; // unsupported
  }

  /**
   * Check if this data type is supported by the native reader.
   */
  public static boolean isSupported(DataType dataType) {
    if (dataType == DataTypes.IntegerType || dataType == DataTypes.LongType
        || dataType == DataTypes.FloatType || dataType == DataTypes.DoubleType) {
      return true;
    }
    if (dataType instanceof DecimalType) {
      return ((DecimalType) dataType).precision() <= 18;
    }
    return false;
  }

  private static long getLongLE(byte[] buf, int offset) {
    return (buf[offset] & 0xFFL)
        | ((buf[offset + 1] & 0xFFL) << 8)
        | ((buf[offset + 2] & 0xFFL) << 16)
        | ((buf[offset + 3] & 0xFFL) << 24)
        | ((buf[offset + 4] & 0xFFL) << 32)
        | ((buf[offset + 5] & 0xFFL) << 40)
        | ((buf[offset + 6] & 0xFFL) << 48)
        | ((buf[offset + 7] & 0xFFL) << 56);
  }
}
