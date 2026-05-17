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
 * Java implementation of FastLanes bitpacking decode for u32 values.
 * Decodes 1024 values per chunk from a bitpacked buffer.
 */
public class FastLanesBitpacking {
  private static final int[] FL_ORDER = {0, 4, 2, 6, 1, 5, 3, 7};
  private static final int T = 32; // bits per u32
  private static final int LANES = 1024 / T; // = 32
  private static final int ELEMS_PER_CHUNK = 1024;

  private static int fastLanesIndex(int row, int lane) {
    int o = row / 8;
    int s = row % 8;
    return (FL_ORDER[o] * 16) + (s * 128) + lane;
  }

  /**
   * Decode 1024 bitpacked u32 values from a FastLanes-encoded buffer.
   *
   * @param packed the packed data (bit_width * 32 u32 words = bit_width * 128 bytes)
   * @param packedOffset byte offset into packed buffer
   * @param bitWidth number of bits per value (1-32)
   * @param output output array (must have at least 1024 elements)
   * @param outputOffset offset into output array
   */
  public static void unpack1024(byte[] packed, int packedOffset, int bitWidth, int[] output,
      int outputOffset) {
    if (bitWidth == 0) {
      for (int i = 0; i < ELEMS_PER_CHUNK; i++) {
        output[outputOffset + i] = 0;
      }
      return;
    }
    if (bitWidth == 32) {
      ByteBuffer buf = ByteBuffer.wrap(packed, packedOffset, ELEMS_PER_CHUNK * 4)
          .order(ByteOrder.LITTLE_ENDIAN);
      for (int lane = 0; lane < LANES; lane++) {
        for (int row = 0; row < T; row++) {
          int idx = fastLanesIndex(row, lane);
          output[outputOffset + idx] = buf.getInt(packedOffset + (LANES * row + lane) * 4);
        }
      }
      return;
    }

    int mask = (1 << bitWidth) - 1;
    for (int lane = 0; lane < LANES; lane++) {
      int src = getIntLE(packed, packedOffset + lane * 4);
      for (int row = 0; row < T; row++) {
        int currWord = (row * bitWidth) / T;
        int shift = (row * bitWidth) % T;
        int nextWord = ((row + 1) * bitWidth) / T;
        int tmp;

        if (nextWord > currWord) {
          int remainingBits = ((row + 1) * bitWidth) % T;
          int currentBits = bitWidth - remainingBits;
          tmp = (src >>> shift) & ((1 << currentBits) - 1);
          if (nextWord < bitWidth) {
            src = getIntLE(packed, packedOffset + (LANES * nextWord + lane) * 4);
            tmp |= (src & ((1 << remainingBits) - 1)) << currentBits;
          }
        } else {
          tmp = (src >>> shift) & mask;
        }

        int idx = fastLanesIndex(row, lane);
        output[outputOffset + idx] = tmp;
      }
    }
  }

  private static int getIntLE(byte[] buf, int offset) {
    return (buf[offset] & 0xFF)
        | ((buf[offset + 1] & 0xFF) << 8)
        | ((buf[offset + 2] & 0xFF) << 16)
        | ((buf[offset + 3] & 0xFF) << 24);
  }
}
