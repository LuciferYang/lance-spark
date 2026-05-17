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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Reads Lance v2 data files using Hadoop FileSystem API. Supports S3, HDFS, local filesystem, etc.
 */
public class LanceFileReader implements AutoCloseable {
  private final FileSystem fs;
  private final Path path;
  private final long fileSize;
  private final LanceFooter footer;
  private final LanceCmoTable cmoTable;

  private LanceFileReader(
      FileSystem fs, Path path, long fileSize, LanceFooter footer, LanceCmoTable cmoTable) {
    this.fs = fs;
    this.path = path;
    this.fileSize = fileSize;
    this.footer = footer;
    this.cmoTable = cmoTable;
  }

  public static LanceFileReader open(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    long fileSize = fs.getFileStatus(path).getLen();

    // Read footer (last 40 bytes)
    byte[] footerBytes = new byte[LanceFooter.FOOTER_SIZE];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(fileSize - LanceFooter.FOOTER_SIZE, footerBytes);
    }
    LanceFooter footer = LanceFooter.parse(footerBytes);

    // Read CMO table
    int cmoSize = footer.getNumColumns() * 16;
    byte[] cmoBytes = new byte[cmoSize];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(footer.getCmoTableOffset(), cmoBytes);
    }
    LanceCmoTable cmoTable = LanceCmoTable.parse(cmoBytes, footer.getNumColumns());

    return new LanceFileReader(fs, path, fileSize, footer, cmoTable);
  }

  public LanceFooter getFooter() {
    return footer;
  }

  public LanceCmoTable getCmoTable() {
    return cmoTable;
  }

  /**
   * Read a column's page data buffers. Returns the raw bytes for buffer 0 (chunk metadata) and
   * buffer 1 (chunk data).
   */
  public ColumnPageData readColumnPage(int columnIndex) throws IOException {
    java.util.List<ColumnPageData> pages = readAllColumnPages(columnIndex);
    return pages.get(0);
  }

  /** Read ALL pages for a column. Returns a list of ColumnPageData (one per page). */
  public java.util.List<ColumnPageData> readAllColumnPages(int columnIndex) throws IOException {
    long colMetaPos = cmoTable.getPosition(columnIndex);
    long colMetaSize = cmoTable.getSize(columnIndex);

    byte[] colMeta = new byte[(int) colMetaSize];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(colMetaPos, colMeta);
    }

    java.util.List<PageInfo> pageInfos = parseAllPages(colMeta);
    java.util.List<ColumnPageData> result = new java.util.ArrayList<>(pageInfos.size());

    try (FSDataInputStream in = fs.open(path)) {
      for (PageInfo pageInfo : pageInfos) {
        byte[] buf0 = new byte[(int) pageInfo.bufferSizes[0]];
        byte[] buf1 = new byte[(int) pageInfo.bufferSizes[1]];
        in.readFully(pageInfo.bufferOffsets[0], buf0);
        in.readFully(pageInfo.bufferOffsets[1], buf1);
        result.add(new ColumnPageData(buf0, buf1, pageInfo.numRows));
      }
    }
    return result;
  }

  private PageInfo parseColumnMetadata(byte[] data) throws IOException {
    return parseAllPages(data).get(0);
  }

  private java.util.List<PageInfo> parseAllPages(byte[] data) throws IOException {
    java.util.List<PageInfo> pages = new java.util.ArrayList<>();
    int pos = 0;
    while (pos < data.length) {
      int tag = readVarint(data, pos);
      pos += varintSize(data, pos);
      int fieldNum = tag >>> 3;
      int wireType = tag & 7;

      if (wireType == 2) {
        int len = readVarint(data, pos);
        pos += varintSize(data, pos);
        if (fieldNum == 2) {
          pages.add(parsePage(data, pos, len));
        }
        pos += len;
      } else if (wireType == 0) {
        readVarint(data, pos);
        pos += varintSize(data, pos);
      } else {
        throw new IOException("Unexpected wire type: " + wireType);
      }
    }
    if (pages.isEmpty()) {
      throw new IOException("No pages found in column metadata");
    }
    return pages;
  }

  private PageInfo parsePage(byte[] data, int start, int len) throws IOException {
    long[] bufferOffsets = null;
    long[] bufferSizes = null;
    long numRows = 0;
    int pos = start;
    int end = start + len;

    while (pos < end) {
      int tag = readVarint(data, pos);
      pos += varintSize(data, pos);
      int fieldNum = tag >>> 3;
      int wireType = tag & 7;

      if (wireType == 2) { // length-delimited (packed repeated)
        int fieldLen = readVarint(data, pos);
        pos += varintSize(data, pos);
        if (fieldNum == 1) { // buffer_offsets
          bufferOffsets = readPackedVarints(data, pos, fieldLen);
        } else if (fieldNum == 2) { // buffer_sizes
          bufferSizes = readPackedVarints(data, pos, fieldLen);
        }
        pos += fieldLen;
      } else if (wireType == 0) { // varint
        long val = readVarintLong(data, pos);
        pos += varintSize(data, pos);
        if (fieldNum == 3) { // length (num_rows)
          numRows = val;
        }
      } else {
        throw new IOException("Unexpected wire type in Page: " + wireType);
      }
    }

    if (bufferOffsets == null || bufferSizes == null) {
      throw new IOException("Missing buffer offsets/sizes in page");
    }
    return new PageInfo(bufferOffsets, bufferSizes, numRows);
  }

  private long[] readPackedVarints(byte[] data, int pos, int len) {
    java.util.List<Long> values = new java.util.ArrayList<>();
    int end = pos + len;
    while (pos < end) {
      values.add(readVarintLong(data, pos));
      pos += varintSize(data, pos);
    }
    return values.stream().mapToLong(Long::longValue).toArray();
  }

  private int readVarint(byte[] data, int pos) {
    return (int) readVarintLong(data, pos);
  }

  private long readVarintLong(byte[] data, int pos) {
    long result = 0;
    int shift = 0;
    while (true) {
      byte b = data[pos++];
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
    }
    return result;
  }

  private int varintSize(byte[] data, int pos) {
    int size = 0;
    while ((data[pos + size] & 0x80) != 0) size++;
    return size + 1;
  }

  @Override
  public void close() throws IOException {
    // FileSystem is shared, don't close it
  }

  public static class ColumnPageData {
    public final byte[] chunkMetadata; // buffer 0
    public final byte[] chunkData; // buffer 1
    public final long numRows;

    public ColumnPageData(byte[] chunkMetadata, byte[] chunkData, long numRows) {
      this.chunkMetadata = chunkMetadata;
      this.chunkData = chunkData;
      this.numRows = numRows;
    }
  }

  private static class PageInfo {
    final long[] bufferOffsets;
    final long[] bufferSizes;
    final long numRows;

    PageInfo(long[] bufferOffsets, long[] bufferSizes, long numRows) {
      this.bufferOffsets = bufferOffsets;
      this.bufferSizes = bufferSizes;
      this.numRows = numRows;
    }
  }
}
