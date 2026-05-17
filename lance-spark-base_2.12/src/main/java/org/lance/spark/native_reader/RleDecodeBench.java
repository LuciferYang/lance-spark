package org.lance.spark.native_reader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Verify RLE decoder correctness and benchmark full decode pipeline
 * (FastLanes + RLE validity) without pre-exported validity bitmap.
 */
public class RleDecodeBench {
  public static void main(String[] args) throws Exception {
    byte[] buf1 = readFile("/tmp/lance_page_buf1.bin");
    byte[] buf0 = readFile("/tmp/lance_page_buf0.bin");

    int numChunks = buf0.length / 4;
    int totalRows = 2000000;

    int[] chunkSizes = new int[numChunks];
    int[] chunkLogValues = new int[numChunks];
    ByteBuffer metaBuf = ByteBuffer.wrap(buf0).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numChunks; i++) {
      int meta = metaBuf.getInt();
      chunkLogValues[i] = meta & 0x0F;
      chunkSizes[i] = ((meta >>> 4) + 1) * 8;
    }

    int[] decodeBuffer = new int[1024];
    boolean[] validityBuffer = new boolean[1024];

    // Verify first chunk
    int defSize = (buf1[2] & 0xFF) | ((buf1[3] & 0xFF) << 8);
    int nullCount = LanceRleDecoder.decodeValidity(buf1, 8, defSize, 1024, validityBuffer, 0);
    System.out.println("First chunk: defSize=" + defSize + ", nullCount=" + nullCount);
    System.out.print("First 20 validity: ");
    for (int i = 0; i < 20; i++) System.out.print(validityBuffer[i] ? "V" : "N");
    System.out.println();
    // Expected: 18 valid, then 1 null, then valid...
    // VVVVVVVVVVVVVVVVVVNV...

    // Full decode benchmark
    System.out.println("\n=== Full pipeline: FastLanes + RLE validity ===");

    // Warmup
    decodeFragment(buf1, chunkSizes, chunkLogValues, numChunks, totalRows,
        decodeBuffer, validityBuffer);

    // Benchmark: 144 fragments
    int iterations = 144;
    long totalSum = 0;
    long totalDecoded = 0;
    long start = System.currentTimeMillis();
    for (int iter = 0; iter < iterations; iter++) {
      totalSum += decodeFragment(buf1, chunkSizes, chunkLogValues, numChunks, totalRows,
          decodeBuffer, validityBuffer);
      totalDecoded += totalRows;
    }
    long elapsed = System.currentTimeMillis() - start;

    System.out.println("Total rows: " + totalDecoded);
    System.out.println("Sum: " + totalSum);
    System.out.println("Time: " + elapsed + "ms");
    System.out.printf("Per row: %.1f ns/row%n", elapsed * 1e6 / totalDecoded);
    System.out.println("\nComparison:");
    System.out.println("  With pre-exported validity: 944ms");
    System.out.println("  Parquet (Spark): ~367ms");
    System.out.println("  Lance JNI (Spark): ~1900ms");
  }

  private static long decodeFragment(byte[] buf1, int[] chunkSizes, int[] chunkLogValues,
      int numChunks, int totalRows, int[] decodeBuffer, boolean[] validityBuffer) {
    long sum = 0;
    int dataOffset = 0;
    int rowsDecoded = 0;

    for (int c = 0; c < numChunks; c++) {
      int chunkSize = chunkSizes[c];
      int numValues = chunkLogValues[c] == 0
          ? totalRows - rowsDecoded
          : (1 << chunkLogValues[c]);

      // Parse chunk header
      int defSize = (buf1[dataOffset + 2] & 0xFF) | ((buf1[dataOffset + 3] & 0xFF) << 8);

      // Decode validity from RLE def levels
      LanceRleDecoder.decodeValidity(buf1, dataOffset + 8, defSize,
          numValues, validityBuffer, 0);

      // Skip to value data
      int offset = dataOffset + 8 + defSize;
      offset = (offset + 7) & ~7;

      // Read bit_width and decode values
      int bitWidth = (buf1[offset] & 0xFF) | ((buf1[offset + 1] & 0xFF) << 8)
          | ((buf1[offset + 2] & 0xFF) << 16) | ((buf1[offset + 3] & 0xFF) << 24);
      offset += 4;

      FastLanesBitpacking.unpack1024(buf1, offset, bitWidth, decodeBuffer, 0);

      // Sum with validity
      for (int i = 0; i < numValues; i++) {
        if (validityBuffer[i]) {
          sum += decodeBuffer[i];
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
