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
import java.util.List;

/**
 * Multi-column Spark PartitionReader using pure Java Lance decode. Reads Lance v2.2 files directly
 * from storage, no JNI/Arrow/tokio.
 */
public class NativeLanceMultiColReader implements PartitionReader<ColumnarBatch> {
  private final StructType schema;
  private final int batchSize;

  // Per-column page data
  private final ColumnState[] columns;
  private final OnHeapColumnVector[] outputColumns;
  private final ColumnarBatch batch;

  private boolean exhausted;
  private final int[] intBuf = new int[1024];
  private final long[] longBuf = new long[1024];
  private final boolean[] validityBuf = new boolean[1024];

  public NativeLanceMultiColReader(
      String dataFilePath,
      int[] columnIndices,
      StructType schema,
      int batchSize,
      Configuration conf)
      throws IOException {
    this.schema = schema;
    this.batchSize = batchSize;

    LanceFileReader fileReader = LanceFileReader.open(new Path(dataFilePath), conf);
    this.columns = new ColumnState[columnIndices.length];
    this.outputColumns = new OnHeapColumnVector[columnIndices.length];

    for (int i = 0; i < columnIndices.length; i++) {
      List<LanceFileReader.ColumnPageData> pages = fileReader.readAllColumnPages(columnIndices[i]);
      DataType dt = schema.fields()[i].dataType();
      columns[i] = new ColumnState(pages, LanceMiniBlockDecoder.getTypeWidth(dt));
      outputColumns[i] = new OnHeapColumnVector(batchSize, dt);
    }
    fileReader.close();

    this.batch = new ColumnarBatch(outputColumns);
    this.exhausted = false;
  }

  @Override
  public boolean next() throws IOException {
    if (exhausted) return false;

    for (OnHeapColumnVector col : outputColumns) col.reset();
    int batchRows = 0;

    // Decode rows from all columns in lockstep
    while (batchRows < batchSize) {
      // Check if first column has more data (all columns have same row count)
      if (columns[0].isExhausted()) {
        break;
      }

      int chunkValues = columns[0].currentChunkValues();
      int toWrite = Math.min(chunkValues, batchSize - batchRows);

      for (int c = 0; c < columns.length; c++) {
        DataType dt = schema.fields()[c].dataType();
        decodeChunkIntoColumn(columns[c], toWrite, outputColumns[c], batchRows, dt);
      }

      // Advance all columns
      for (ColumnState col : columns) {
        col.advance(toWrite);
      }
      batchRows += toWrite;
    }

    if (batchRows == 0) {
      exhausted = true;
      return false;
    }
    batch.setNumRows(batchRows);
    return true;
  }

  private void decodeChunkIntoColumn(
      ColumnState state, int numValues, OnHeapColumnVector col, int colOffset, DataType dataType) {
    byte[] data = state.currentPageData();
    int offset = state.currentDataOffset();
    int typeWidth = state.typeWidth;

    // Parse chunk header
    int numLevels = (data[offset] & 0xFF) | ((data[offset + 1] & 0xFF) << 8);
    int valOffset;
    if (numLevels > 0) {
      int defSize = (data[offset + 2] & 0xFF) | ((data[offset + 3] & 0xFF) << 8);
      LanceRleDecoder.decodeValidity(data, offset + 8, defSize, numValues, validityBuf, 0);
      valOffset = offset + 8 + defSize;
      valOffset = (valOffset + 7) & ~7;
    } else {
      for (int i = 0; i < numValues; i++) validityBuf[i] = true;
      valOffset = offset + 8;
    }

    if (typeWidth == 4) {
      int bitWidth = getI32(data, valOffset);
      valOffset += 4;
      FastLanesBitpacking.unpack1024(data, valOffset, bitWidth, intBuf, 0);
      for (int i = 0; i < numValues; i++) {
        if (validityBuf[i]) col.putInt(colOffset + i, intBuf[i]);
        else col.putNull(colOffset + i);
      }
    } else if (typeWidth == 8) {
      int bitWidth = (int) getI64(data, valOffset);
      valOffset += 8;
      FastLanesBitpacking.unpack1024Long(data, valOffset, bitWidth, longBuf, 0);
      for (int i = 0; i < numValues; i++) {
        if (validityBuf[i]) col.putLong(colOffset + i, longBuf[i]);
        else col.putNull(colOffset + i);
      }
    } else if (typeWidth == 16) {
      for (int i = 0; i < numValues; i++) {
        if (validityBuf[i]) col.putLong(colOffset + i, getI64(data, valOffset + i * 16));
        else col.putNull(colOffset + i);
      }
    }
  }

  @Override
  public ColumnarBatch get() {
    return batch;
  }

  @Override
  public void close() throws IOException {
    for (OnHeapColumnVector col : outputColumns) col.close();
  }

  private static int getI32(byte[] b, int o) {
    return (b[o] & 0xFF)
        | ((b[o + 1] & 0xFF) << 8)
        | ((b[o + 2] & 0xFF) << 16)
        | ((b[o + 3] & 0xFF) << 24);
  }

  private static long getI64(byte[] b, int o) {
    return (b[o] & 0xFFL)
        | ((b[o + 1] & 0xFFL) << 8)
        | ((b[o + 2] & 0xFFL) << 16)
        | ((b[o + 3] & 0xFFL) << 24)
        | ((b[o + 4] & 0xFFL) << 32)
        | ((b[o + 5] & 0xFFL) << 40)
        | ((b[o + 6] & 0xFFL) << 48)
        | ((b[o + 7] & 0xFFL) << 56);
  }

  /** Tracks decode state for one column across multiple pages. */
  private static class ColumnState {
    final List<LanceFileReader.ColumnPageData> pages;
    final int typeWidth;
    int currentPage;
    int currentChunk;
    int dataOffset;
    int rowsDecodedInPage;
    int[] chunkSizes;
    int[] chunkLogValues;
    int numChunks;
    int pageRows;

    ColumnState(List<LanceFileReader.ColumnPageData> pages, int typeWidth) {
      this.pages = pages;
      this.typeWidth = typeWidth;
      this.currentPage = 0;
      loadPage(0);
    }

    private void loadPage(int pageIdx) {
      if (pageIdx >= pages.size()) return;
      LanceFileReader.ColumnPageData page = pages.get(pageIdx);
      numChunks = page.chunkMetadata.length / 4;
      chunkSizes = new int[numChunks];
      chunkLogValues = new int[numChunks];
      ByteBuffer buf = ByteBuffer.wrap(page.chunkMetadata).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < numChunks; i++) {
        int meta = buf.getInt();
        chunkLogValues[i] = meta & 0x0F;
        chunkSizes[i] = ((meta >>> 4) + 1) * 8;
      }
      pageRows = (int) page.numRows;
      currentChunk = 0;
      dataOffset = 0;
      rowsDecodedInPage = 0;
    }

    boolean isExhausted() {
      return currentPage >= pages.size();
    }

    int currentChunkValues() {
      if (isExhausted()) return 0;
      return chunkLogValues[currentChunk] == 0
          ? pageRows - rowsDecodedInPage
          : (1 << chunkLogValues[currentChunk]);
    }

    byte[] currentPageData() {
      return pages.get(currentPage).chunkData;
    }

    int currentDataOffset() {
      return dataOffset;
    }

    void advance(int numValues) {
      rowsDecodedInPage += numValues;
      dataOffset += chunkSizes[currentChunk];
      currentChunk++;
      if (currentChunk >= numChunks) {
        currentPage++;
        if (currentPage < pages.size()) {
          loadPage(currentPage);
        }
      }
    }
  }
}
