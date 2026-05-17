package org.lance.spark.native_reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Verify multi-type decode: int32 (ss_quantity col 10) and int64 (ss_item_sk col 2).
 */
public class MultiTypeVerifyTest {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.s3a.endpoint", "http://localhost:9000");
    conf.set("fs.s3a.access.key", "minioadmin");
    conf.set("fs.s3a.secret.key", "minioadmin");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");

    String filePath = "s3a://benchmark/tpcds-sf-100/store_sales.lance/data/"
        + "10111110011001111000001116c9f243f8b974c6b80dafd4a1.lance";

    LanceFileReader reader = LanceFileReader.open(new Path(filePath), conf);
    System.out.println("Footer: " + reader.getFooter());

    // Test 1: int32 (ss_quantity, col 10)
    System.out.println("\n=== Column 10: ss_quantity (int32) ===");
    testColumn(reader, 10, 4, 96489628L, "int32");

    // Test 2: int64 (ss_item_sk, col 2)
    System.out.println("\n=== Column 2: ss_item_sk (int64) ===");
    testColumn(reader, 2, 8, 204007946651L, "int64");

    reader.close();
  }

  private static void testColumn(LanceFileReader reader, int colIdx, int typeWidth,
      long expectedSum, String typeName) throws Exception {
    LanceFileReader.ColumnPageData pageData = reader.readColumnPage(colIdx);
    System.out.println("  Chunk metadata: " + pageData.chunkMetadata.length + " bytes");
    System.out.println("  Chunk data: " + pageData.chunkData.length + " bytes");
    System.out.println("  Rows: " + pageData.numRows);

    int numChunks = pageData.chunkMetadata.length / 4;
    int[] chunkSizes = new int[numChunks];
    int[] chunkLogValues = new int[numChunks];
    ByteBuffer metaBuf = ByteBuffer.wrap(pageData.chunkMetadata).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numChunks; i++) {
      int meta = metaBuf.getInt();
      chunkLogValues[i] = meta & 0x0F;
      chunkSizes[i] = ((meta >>> 4) + 1) * 8;
    }

    int totalRows = (int) pageData.numRows;
    long sum = 0;
    int nullCount = 0;
    int dataOffset = 0;
    int rowsDecoded = 0;

    int[] intBuf = new int[1024];
    long[] longBuf = new long[1024];
    boolean[] validity = new boolean[1024];

    for (int c = 0; c < numChunks; c++) {
      int numValues = chunkLogValues[c] == 0
          ? totalRows - rowsDecoded
          : (1 << chunkLogValues[c]);

      // Parse chunk header - format depends on nullable
      int numLevels = (pageData.chunkData[dataOffset] & 0xFF)
          | ((pageData.chunkData[dataOffset + 1] & 0xFF) << 8);

      int offset;
      if (numLevels > 0) {
        // Nullable: [num_levels: u16][def_size: u16][value_buf_size: u32][pad][def][pad][value]
        int defSize = (pageData.chunkData[dataOffset + 2] & 0xFF)
            | ((pageData.chunkData[dataOffset + 3] & 0xFF) << 8);
        int nulls = LanceRleDecoder.decodeValidity(
            pageData.chunkData, dataOffset + 8, defSize, numValues, validity, 0);
        nullCount += nulls;
        offset = dataOffset + 8 + defSize;
        offset = (offset + 7) & ~7;
      } else {
        // Non-nullable: [num_levels: u16 = 0][value_buf_size: u32][padding][value]
        for (int i = 0; i < numValues; i++) validity[i] = true;
        offset = dataOffset + 8; // 2 + 4 + 2 padding = 8
      }

      if (typeWidth == 4) {
        int bitWidth = getInt(pageData.chunkData, offset);
        offset += 4;
        FastLanesBitpacking.unpack1024(pageData.chunkData, offset, bitWidth, intBuf, 0);
        for (int i = 0; i < numValues; i++) {
          if (validity[i]) sum += intBuf[i];
        }
      } else if (typeWidth == 8) {
        long bitWidthLong = getLong(pageData.chunkData, offset);
        int bitWidth = (int) bitWidthLong;
        offset += 8;
        FastLanesBitpacking.unpack1024Long(pageData.chunkData, offset, bitWidth, longBuf, 0);
        for (int i = 0; i < numValues; i++) {
          if (validity[i]) sum += longBuf[i];
        }
      }

      rowsDecoded += numValues;
      dataOffset += chunkSizes[c];
    }

    System.out.println("  Decoded rows: " + rowsDecoded);
    System.out.println("  Null count: " + nullCount);
    System.out.println("  Sum: " + sum);
    System.out.println("  Expected: " + expectedSum);
    System.out.println("  Match: " + (sum == expectedSum));
  }

  private static int getInt(byte[] buf, int offset) {
    return (buf[offset] & 0xFF) | ((buf[offset + 1] & 0xFF) << 8)
        | ((buf[offset + 2] & 0xFF) << 16) | ((buf[offset + 3] & 0xFF) << 24);
  }

  private static long getLong(byte[] buf, int offset) {
    return (buf[offset] & 0xFFL) | ((buf[offset + 1] & 0xFFL) << 8)
        | ((buf[offset + 2] & 0xFFL) << 16) | ((buf[offset + 3] & 0xFFL) << 24)
        | ((buf[offset + 4] & 0xFFL) << 32) | ((buf[offset + 5] & 0xFFL) << 40)
        | ((buf[offset + 6] & 0xFFL) << 48) | ((buf[offset + 7] & 0xFFL) << 56);
  }
}
