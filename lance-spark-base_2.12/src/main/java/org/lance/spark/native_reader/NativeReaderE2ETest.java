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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * End-to-end test: read Lance file from S3, decode with Java-native reader, verify correctness and
 * measure performance.
 */
public class NativeReaderE2ETest {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.s3a.endpoint", "http://localhost:9000");
    conf.set("fs.s3a.access.key", "minioadmin");
    conf.set("fs.s3a.secret.key", "minioadmin");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");

    // Fragment 0 data file
    String filePath =
        "s3a://benchmark/tpcds-sf-100/store_sales.lance/data/"
            + "10111110011001111000001116c9f243f8b974c6b80dafd4a1.lance";

    System.out.println("Opening: " + filePath);
    LanceFileReader reader = LanceFileReader.open(new Path(filePath), conf);
    System.out.println("Footer: " + reader.getFooter());

    // Read ss_quantity (column 10)
    System.out.println("\nReading column 10 (ss_quantity)...");
    LanceFileReader.ColumnPageData pageData = reader.readColumnPage(10);
    System.out.println("Chunk metadata: " + pageData.chunkMetadata.length + " bytes");
    System.out.println("Chunk data: " + pageData.chunkData.length + " bytes");
    System.out.println("Rows: " + pageData.numRows);

    // Decode all chunks
    int numChunks = pageData.chunkMetadata.length / 4;
    int[] chunkSizes = new int[numChunks];
    int[] chunkLogValues = new int[numChunks];
    ByteBuffer metaBuf = ByteBuffer.wrap(pageData.chunkMetadata).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numChunks; i++) {
      int meta = metaBuf.getInt();
      chunkLogValues[i] = meta & 0x0F;
      chunkSizes[i] = ((meta >>> 4) + 1) * 8;
    }

    int[] decodeBuffer = new int[1024];
    boolean[] validityBuffer = new boolean[1024];
    long sum = 0;
    int totalNulls = 0;
    int dataOffset = 0;
    int rowsDecoded = 0;
    int totalRows = (int) pageData.numRows;

    for (int c = 0; c < numChunks; c++) {
      int numValues = chunkLogValues[c] == 0 ? totalRows - rowsDecoded : (1 << chunkLogValues[c]);

      int defSize =
          (pageData.chunkData[dataOffset + 2] & 0xFF)
              | ((pageData.chunkData[dataOffset + 3] & 0xFF) << 8);

      // Decode validity
      int nulls =
          LanceRleDecoder.decodeValidity(
              pageData.chunkData, dataOffset + 8, defSize, numValues, validityBuffer, 0);
      totalNulls += nulls;

      // Decode values
      int offset = dataOffset + 8 + defSize;
      offset = (offset + 7) & ~7;
      int bitWidth =
          (pageData.chunkData[offset] & 0xFF)
              | ((pageData.chunkData[offset + 1] & 0xFF) << 8)
              | ((pageData.chunkData[offset + 2] & 0xFF) << 16)
              | ((pageData.chunkData[offset + 3] & 0xFF) << 24);
      offset += 4;

      FastLanesBitpacking.unpack1024(pageData.chunkData, offset, bitWidth, decodeBuffer, 0);

      for (int i = 0; i < numValues; i++) {
        if (validityBuffer[i]) {
          sum += decodeBuffer[i];
        }
      }

      rowsDecoded += numValues;
      dataOffset += chunkSizes[c];
    }

    System.out.println("\nResults:");
    System.out.println("  Rows decoded: " + rowsDecoded);
    System.out.println("  Null count: " + totalNulls);
    System.out.println("  Sum: " + sum);
    System.out.println("  Expected sum (from Python): 96489628");
    System.out.println("  Match: " + (sum == 96489628L));

    reader.close();
  }
}
