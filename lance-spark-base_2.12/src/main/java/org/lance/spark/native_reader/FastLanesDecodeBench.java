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
package org.lance.spark.native_reader;

import java.io.*;
import java.nio.*;

/**
 * Benchmark: Java FastLanes bitpacking decode performance. Reads the raw miniblock data from a
 * Lance page and decodes all chunks.
 */
public class FastLanesDecodeBench {
  public static void main(String[] args) throws Exception {
    byte[] buf1 = readFile("/tmp/lance_page_buf1.bin");
    byte[] buf0 = readFile("/tmp/lance_page_buf0.bin");
    byte[] validity = readFile("/tmp/lance_frag0_validity.bin");

    int numChunks = buf0.length / 4;
    int totalRows = 2000000;
    System.out.println(
        "Chunks: "
            + numChunks
            + ", Value buffer: "
            + buf1.length
            + " bytes, Validity: "
            + validity.length
            + " bytes");

    int[] chunkSizes = new int[numChunks];
    int[] chunkLogValues = new int[numChunks];
    ByteBuffer metaBuf = ByteBuffer.wrap(buf0).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numChunks; i++) {
      int meta = metaBuf.getInt();
      chunkLogValues[i] = meta & 0x0F;
      chunkSizes[i] = ((meta >>> 4) + 1) * 8;
    }

    int[] output = new int[1024];

    // Warmup
    decodeAndSum(buf1, chunkSizes, chunkLogValues, numChunks, output, totalRows, validity);

    // Benchmark: 144 iterations = 288M rows
    int iterations = 144;
    long totalSum = 0;
    long totalDecoded = 0;
    long start = System.currentTimeMillis();
    for (int iter = 0; iter < iterations; iter++) {
      totalSum +=
          decodeAndSum(buf1, chunkSizes, chunkLogValues, numChunks, output, totalRows, validity);
      totalDecoded += totalRows;
    }
    long elapsed = System.currentTimeMillis() - start;

    System.out.println("Total rows: " + totalDecoded);
    System.out.println("Sum: " + totalSum);
    System.out.println("Time: " + elapsed + "ms");
    System.out.printf("Per row: %.1f ns/row%n", elapsed * 1e6 / totalDecoded);
    System.out.println("\nComparison:");
    System.out.println("  Parquet (Spark):     ~367ms / 1.3 ns/row");
    System.out.println("  Lance JNI (Spark):  ~1900ms / 6.6 ns/row");
  }

  private static long decodeAndSum(
      byte[] buf1,
      int[] chunkSizes,
      int[] chunkLogValues,
      int numChunks,
      int[] output,
      int totalRows,
      byte[] validity) {
    long sum = 0;
    int dataOffset = 0;
    int rowsDecoded = 0;

    for (int c = 0; c < numChunks; c++) {
      int chunkSize = chunkSizes[c];
      int numValues = chunkLogValues[c] == 0 ? totalRows - rowsDecoded : (1 << chunkLogValues[c]);

      // Parse chunk header
      int defSize = (buf1[dataOffset + 2] & 0xFF) | ((buf1[dataOffset + 3] & 0xFF) << 8);
      int valueBufSize =
          (buf1[dataOffset + 4] & 0xFF)
              | ((buf1[dataOffset + 5] & 0xFF) << 8)
              | ((buf1[dataOffset + 6] & 0xFF) << 16)
              | ((buf1[dataOffset + 7] & 0xFF) << 24);

      // Skip to value data
      int offset = dataOffset + 8 + defSize;
      offset = (offset + 7) & ~7;

      // Read bit_width
      int bitWidth =
          (buf1[offset] & 0xFF)
              | ((buf1[offset + 1] & 0xFF) << 8)
              | ((buf1[offset + 2] & 0xFF) << 16)
              | ((buf1[offset + 3] & 0xFF) << 24);
      offset += 4;

      // Decode
      FastLanesBitpacking.unpack1024(buf1, offset, bitWidth, output, 0);

      // Sum with validity check
      for (int i = 0; i < numValues; i++) {
        int globalRow = rowsDecoded + i;
        int byteIdx = globalRow >>> 3;
        int bitIdx = globalRow & 7;
        if ((validity[byteIdx] & (1 << bitIdx)) != 0) {
          sum += output[i];
        }
      }

      rowsDecoded += numValues;
      dataOffset += chunkSize;
    }
    return sum;
  }

  private static byte[] readFile(String path) throws IOException {
    File f = new File(path);
    byte[] data = new byte[(int) f.length()];
    try (FileInputStream fis = new FileInputStream(f)) {
      int read = 0;
      while (read < data.length) {
        read += fis.read(data, read, data.length - read);
      }
    }
    return data;
  }
}
