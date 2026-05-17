/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.lance.spark.native_reader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Decodes RLE-encoded def levels from a Lance miniblock chunk.
 *
 * <p>Format: [values_size: u64][values_raw: u16[]][lengths_raw: u8[]]
 * where values_raw contains the run values and lengths_raw contains run lengths.
 * For simple nullable columns: value 0 = valid, value 1 = null.
 */
public class LanceRleDecoder {

  /**
   * Decode RLE def levels and write validity into a boolean array.
   *
   * @param defData the raw def level data blob
   * @param defOffset offset into defData
   * @param defSize size of the def data
   * @param numValues number of values to decode
   * @param validityOut output: true = valid, false = null
   * @param validityOffset offset into validityOut
   * @return number of nulls found
   */
  public static int decodeValidity(byte[] defData, int defOffset, int defSize,
      int numValues, boolean[] validityOut, int validityOffset) {
    // [values_size: u64][values_raw: u16[]][lengths_raw: u8[]]
    long valuesSize = getU64LE(defData, defOffset);
    int valuesStart = defOffset + 8;
    int lengthsStart = valuesStart + (int) valuesSize;
    int lengthsSize = defSize - 8 - (int) valuesSize;

    int numRuns = (int) valuesSize / 2; // u16 values
    int nullCount = 0;
    int outIdx = validityOffset;
    int remaining = numValues;

    for (int r = 0; r < numRuns && remaining > 0; r++) {
      int value = getU16LE(defData, valuesStart + r * 2);
      int length = defData[lengthsStart + r] & 0xFF;
      int writeLen = Math.min(length, remaining);

      boolean isValid = (value == 0); // def_level 0 = valid, 1 = null
      for (int i = 0; i < writeLen; i++) {
        validityOut[outIdx++] = isValid;
      }
      if (!isValid) {
        nullCount += writeLen;
      }
      remaining -= writeLen;
    }

    return nullCount;
  }

  private static long getU64LE(byte[] buf, int offset) {
    return (buf[offset] & 0xFFL)
        | ((buf[offset + 1] & 0xFFL) << 8)
        | ((buf[offset + 2] & 0xFFL) << 16)
        | ((buf[offset + 3] & 0xFFL) << 24)
        | ((buf[offset + 4] & 0xFFL) << 32)
        | ((buf[offset + 5] & 0xFFL) << 40)
        | ((buf[offset + 6] & 0xFFL) << 48)
        | ((buf[offset + 7] & 0xFFL) << 56);
  }

  private static int getU16LE(byte[] buf, int offset) {
    return (buf[offset] & 0xFF) | ((buf[offset + 1] & 0xFF) << 8);
  }
}
