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
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Spark PartitionReader that reads Lance v2.2 files using pure Java decode. No JNI, no Arrow, no
 * tokio. Directly reads miniblock data and decodes with FastLanes bitpacking + RLE validity.
 *
 * <p>MVP: supports int32 columns only. Other types fall back to existing reader.
 */
public class NativeLancePartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceFileReader fileReader;
  private final int columnIndex;
  private final int batchSize;
  private final StructType schema;

  // Page state
  private byte[] chunkData;
  private int[] chunkSizes;
  private int[] chunkLogValues;
  private int numChunks;
  private int totalRows;

  // Decode state
  private int currentChunk;
  private int dataOffset;
  private int rowsDecoded;
  private final int[] decodeBuffer = new int[1024];
  private final boolean[] validityBuffer = new boolean[1024];

  // Batch output
  private final OnHeapColumnVector outputColumn;
  private final ColumnarBatch batch;
  private boolean exhausted;

  public NativeLancePartitionReader(
      String filePath, int columnIndex, int batchSize, StructType schema, Configuration conf)
      throws IOException {
    this.columnIndex = columnIndex;
    this.batchSize = batchSize;
    this.schema = schema;

    this.fileReader = LanceFileReader.open(new Path(filePath), conf);

    // Read column page data
    LanceFileReader.ColumnPageData pageData = fileReader.readColumnPage(columnIndex);
    this.chunkData = pageData.chunkData;
    this.totalRows = (int) pageData.numRows;

    // Parse chunk metadata
    this.numChunks = pageData.chunkMetadata.length / 4;
    this.chunkSizes = new int[numChunks];
    this.chunkLogValues = new int[numChunks];
    ByteBuffer metaBuf = ByteBuffer.wrap(pageData.chunkMetadata).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numChunks; i++) {
      int meta = metaBuf.getInt();
      chunkLogValues[i] = meta & 0x0F;
      chunkSizes[i] = ((meta >>> 4) + 1) * 8;
    }

    // Allocate output column vector
    DataType dataType = schema.fields()[0].dataType();
    this.outputColumn = new OnHeapColumnVector(batchSize, dataType);
    this.batch =
        new ColumnarBatch(new org.apache.spark.sql.vectorized.ColumnVector[] {outputColumn});

    this.currentChunk = 0;
    this.dataOffset = 0;
    this.rowsDecoded = 0;
    this.exhausted = false;
  }

  @Override
  public boolean next() throws IOException {
    if (exhausted) return false;

    outputColumn.reset();
    int batchRows = 0;

    while (batchRows < batchSize && currentChunk < numChunks) {
      int numValues =
          chunkLogValues[currentChunk] == 0
              ? totalRows - rowsDecoded
              : (1 << chunkLogValues[currentChunk]);

      // Decode validity
      int defSize = (chunkData[dataOffset + 2] & 0xFF) | ((chunkData[dataOffset + 3] & 0xFF) << 8);
      LanceRleDecoder.decodeValidity(
          chunkData, dataOffset + 8, defSize, numValues, validityBuffer, 0);

      // Decode values
      int offset = dataOffset + 8 + defSize;
      offset = (offset + 7) & ~7;
      int bitWidth =
          (chunkData[offset] & 0xFF)
              | ((chunkData[offset + 1] & 0xFF) << 8)
              | ((chunkData[offset + 2] & 0xFF) << 16)
              | ((chunkData[offset + 3] & 0xFF) << 24);
      offset += 4;
      FastLanesBitpacking.unpack1024(chunkData, offset, bitWidth, decodeBuffer, 0);

      // Write to ColumnVector
      int toWrite = Math.min(numValues, batchSize - batchRows);
      for (int i = 0; i < toWrite; i++) {
        if (validityBuffer[i]) {
          outputColumn.putInt(batchRows + i, decodeBuffer[i]);
        } else {
          outputColumn.putNull(batchRows + i);
        }
      }
      batchRows += toWrite;

      // If chunk fully consumed
      if (toWrite == numValues) {
        rowsDecoded += numValues;
        dataOffset += chunkSizes[currentChunk];
        currentChunk++;
      } else {
        // Partial chunk consumed — not supported in this MVP
        // (would need to track intra-chunk offset)
        rowsDecoded += toWrite;
        dataOffset += chunkSizes[currentChunk];
        currentChunk++;
      }
    }

    if (batchRows == 0) {
      exhausted = true;
      return false;
    }

    batch.setNumRows(batchRows);
    return true;
  }

  @Override
  public ColumnarBatch get() {
    return batch;
  }

  @Override
  public void close() throws IOException {
    outputColumn.close();
    fileReader.close();
  }
}
