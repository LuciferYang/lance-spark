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

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataTypes;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Standalone benchmark that simulates a Spark PartitionReader using Java-native Lance decode. Reads
 * miniblock data, decodes with FastLanes, writes to OnHeapColumnVector, and computes sum (like
 * Spark codegen would).
 *
 * <p>This proves the end-to-end performance of the Java-native approach including Spark's
 * ColumnVector overhead.
 */
public class NativeReaderSparkBench {
  public static void main(String[] args) throws Exception {
    byte[] buf1 = readFile("/tmp/lance_page_buf1.bin");
    byte[] buf0 = readFile("/tmp/lance_page_buf0.bin");
    byte[] validity = readFile("/tmp/lance_frag0_validity.bin");

    int numChunks = buf0.length / 4;
    int totalFragRows = 2000000;
    int batchSize = 65536;

    int[] chunkSizes = new int[numChunks];
    int[] chunkLogValues = new int[numChunks];
    ByteBuffer metaBuf = ByteBuffer.wrap(buf0).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numChunks; i++) {
      int meta = metaBuf.getInt();
      chunkLogValues[i] = meta & 0x0F;
      chunkSizes[i] = ((meta >>> 4) + 1) * 8;
    }

    // Pre-allocate ColumnVector (reused across batches, like Parquet does)
    OnHeapColumnVector col = new OnHeapColumnVector(batchSize, DataTypes.IntegerType);
    int[] decodeBuffer = new int[1024];

    // Warmup
    simulateSparkScan(
        buf1,
        buf0,
        chunkSizes,
        chunkLogValues,
        numChunks,
        validity,
        totalFragRows,
        batchSize,
        col,
        decodeBuffer);

    // Benchmark: 144 fragments = 288M rows
    int iterations = 144;
    long totalSum = 0;
    long totalRows = 0;
    long start = System.currentTimeMillis();
    for (int iter = 0; iter < iterations; iter++) {
      totalSum +=
          simulateSparkScan(
              buf1,
              buf0,
              chunkSizes,
              chunkLogValues,
              numChunks,
              validity,
              totalFragRows,
              batchSize,
              col,
              decodeBuffer);
      totalRows += totalFragRows;
    }
    long elapsed = System.currentTimeMillis() - start;

    System.out.println("Total rows: " + totalRows);
    System.out.println("Sum: " + totalSum);
    System.out.println("Time: " + elapsed + "ms");
    System.out.printf("Per row: %.1f ns/row%n", elapsed * 1e6 / totalRows);
    System.out.println("\nComparison:");
    System.out.println("  This (native decode + OnHeapColumnVector): " + elapsed + "ms");
    System.out.println("  Parquet (Spark):     ~367ms / 1.3 ns/row");
    System.out.println("  Lance JNI (Spark):  ~1900ms / 6.6 ns/row");
  }

  private static long simulateSparkScan(
      byte[] buf1,
      byte[] buf0,
      int[] chunkSizes,
      int[] chunkLogValues,
      int numChunks,
      byte[] validity,
      int totalFragRows,
      int batchSize,
      OnHeapColumnVector col,
      int[] decodeBuffer) {
    long sum = 0;
    int dataOffset = 0;
    int rowsDecoded = 0;
    int batchRows = 0;

    for (int c = 0; c < numChunks; c++) {
      int chunkSize = chunkSizes[c];
      int numValues =
          chunkLogValues[c] == 0 ? totalFragRows - rowsDecoded : (1 << chunkLogValues[c]);

      // Parse chunk header
      int defSize = (buf1[dataOffset + 2] & 0xFF) | ((buf1[dataOffset + 3] & 0xFF) << 8);

      // Skip to value data
      int offset = dataOffset + 8 + defSize;
      offset = (offset + 7) & ~7;

      // Read bit_width and decode
      int bitWidth =
          (buf1[offset] & 0xFF)
              | ((buf1[offset + 1] & 0xFF) << 8)
              | ((buf1[offset + 2] & 0xFF) << 16)
              | ((buf1[offset + 3] & 0xFF) << 24);
      offset += 4;

      FastLanesBitpacking.unpack1024(buf1, offset, bitWidth, decodeBuffer, 0);

      // Write to OnHeapColumnVector (like Spark PartitionReader would)
      for (int i = 0; i < numValues; i++) {
        int globalRow = rowsDecoded + i;
        int byteIdx = globalRow >>> 3;
        int bitIdx = globalRow & 7;
        boolean isNull = (validity[byteIdx] & (1 << bitIdx)) == 0;

        if (batchRows >= batchSize) {
          // Simulate batch boundary: Spark would consume this batch
          sum += consumeBatch(col, batchRows);
          col.reset();
          batchRows = 0;
        }

        if (isNull) {
          col.putNull(batchRows);
        } else {
          col.putInt(batchRows, decodeBuffer[i]);
        }
        batchRows++;
      }

      rowsDecoded += numValues;
      dataOffset += chunkSize;
    }

    // Consume remaining rows
    if (batchRows > 0) {
      sum += consumeBatch(col, batchRows);
      col.reset();
    }
    return sum;
  }

  private static long consumeBatch(OnHeapColumnVector col, int numRows) {
    // Simulate Spark codegen: sum all non-null values
    long sum = 0;
    for (int i = 0; i < numRows; i++) {
      if (!col.isNullAt(i)) {
        sum += col.getInt(i);
      }
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
