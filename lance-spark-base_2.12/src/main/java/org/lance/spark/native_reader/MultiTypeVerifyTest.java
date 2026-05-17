package org.lance.spark.native_reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Verify multi-type, multi-page decode correctness.
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

    // int32 nullable (1 page, 2M rows)
    System.out.println("\n=== Column 10: ss_quantity (int32, nullable) ===");
    testColumn(reader, 10, 4, 96489628L);

    // int64 non-nullable (2 pages, 2M rows total)
    System.out.println("\n=== Column 2: ss_item_sk (int64, non-nullable, 2 pages) ===");
    testColumn(reader, 2, 8, 204007946651L);

    // decimal128 nullable (4 pages, 2M rows total)
    System.out.println("\n=== Column 15: ss_ext_sales_price (decimal128, nullable, 4 pages) ===");
    testColumn(reader, 15, 16, 365313864199L);

    reader.close();
  }

  private static void testColumn(LanceFileReader reader, int colIdx, int typeWidth,
      long expectedSum) throws Exception {
    List<LanceFileReader.ColumnPageData> pages = reader.readAllColumnPages(colIdx);
    System.out.println("  Pages: " + pages.size());

    long sum = 0;
    int totalNulls = 0;
    int totalRows = 0;
    int[] intBuf = new int[1024];
    long[] longBuf = new long[1024];
    boolean[] validity = new boolean[1024];

    for (LanceFileReader.ColumnPageData page : pages) {
      int numChunks = page.chunkMetadata.length / 4;
      int[] chunkSizes = new int[numChunks];
      int[] chunkLogValues = new int[numChunks];
      ByteBuffer metaBuf = ByteBuffer.wrap(page.chunkMetadata).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < numChunks; i++) {
        int meta = metaBuf.getInt();
        chunkLogValues[i] = meta & 0x0F;
        chunkSizes[i] = ((meta >>> 4) + 1) * 8;
      }

      int pageRows = (int) page.numRows;
      int dataOffset = 0;
      int rowsDecoded = 0;

      for (int c = 0; c < numChunks; c++) {
        int numValues = chunkLogValues[c] == 0
            ? pageRows - rowsDecoded : (1 << chunkLogValues[c]);

        int numLevels = getU16(page.chunkData, dataOffset);
        int offset;
        if (numLevels > 0) {
          int defSize = getU16(page.chunkData, dataOffset + 2);
          totalNulls += LanceRleDecoder.decodeValidity(
              page.chunkData, dataOffset + 8, defSize, numValues, validity, 0);
          offset = dataOffset + 8 + defSize;
          offset = (offset + 7) & ~7;
        } else {
          for (int i = 0; i < numValues; i++) validity[i] = true;
          offset = dataOffset + 8;
        }

        if (typeWidth == 4) {
          int bitWidth = getI32(page.chunkData, offset);
          offset += 4;
          FastLanesBitpacking.unpack1024(page.chunkData, offset, bitWidth, intBuf, 0);
          for (int i = 0; i < numValues; i++)
            if (validity[i]) sum += intBuf[i];
        } else if (typeWidth == 8) {
          int bitWidth = (int) getI64(page.chunkData, offset);
          offset += 8;
          FastLanesBitpacking.unpack1024Long(page.chunkData, offset, bitWidth, longBuf, 0);
          for (int i = 0; i < numValues; i++)
            if (validity[i]) sum += longBuf[i];
        } else if (typeWidth == 16) {
          for (int i = 0; i < numValues; i++)
            if (validity[i]) sum += getI64(page.chunkData, offset + i * 16);
        }

        rowsDecoded += numValues;
        dataOffset += chunkSizes[c];
      }
      totalRows += rowsDecoded;
    }

    System.out.println("  Total rows: " + totalRows);
    System.out.println("  Null count: " + totalNulls);
    System.out.println("  Sum: " + sum);
    System.out.println("  Expected: " + expectedSum);
    System.out.println("  Match: " + (sum == expectedSum));
  }

  private static int getU16(byte[] b, int o) {
    return (b[o] & 0xFF) | ((b[o+1] & 0xFF) << 8);
  }
  private static int getI32(byte[] b, int o) {
    return (b[o]&0xFF)|((b[o+1]&0xFF)<<8)|((b[o+2]&0xFF)<<16)|((b[o+3]&0xFF)<<24);
  }
  private static long getI64(byte[] b, int o) {
    return (b[o]&0xFFL)|((b[o+1]&0xFFL)<<8)|((b[o+2]&0xFFL)<<16)|((b[o+3]&0xFFL)<<24)
        |((b[o+4]&0xFFL)<<32)|((b[o+5]&0xFFL)<<40)|((b[o+6]&0xFFL)<<48)|((b[o+7]&0xFFL)<<56);
  }
}
