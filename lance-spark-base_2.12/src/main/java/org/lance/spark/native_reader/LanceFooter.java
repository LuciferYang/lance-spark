/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.lance.spark.native_reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Java-native Lance v2 file footer parser.
 * Reads the last 40 bytes of a Lance data file to extract metadata offsets.
 */
public class LanceFooter {
  public static final byte[] MAGIC = {'L', 'A', 'N', 'C'};
  public static final int FOOTER_SIZE = 40;

  private final long columnMeta0Offset;
  private final long cmoTableOffset;
  private final long gboTableOffset;
  private final int numGlobalBuffers;
  private final int numColumns;
  private final int majorVersion;
  private final int minorVersion;

  private LanceFooter(long columnMeta0Offset, long cmoTableOffset, long gboTableOffset,
      int numGlobalBuffers, int numColumns, int majorVersion, int minorVersion) {
    this.columnMeta0Offset = columnMeta0Offset;
    this.cmoTableOffset = cmoTableOffset;
    this.gboTableOffset = gboTableOffset;
    this.numGlobalBuffers = numGlobalBuffers;
    this.numColumns = numColumns;
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
  }

  public static LanceFooter parse(byte[] footerBytes) throws IOException {
    if (footerBytes.length < FOOTER_SIZE) {
      throw new IOException("Footer too small: " + footerBytes.length);
    }
    int offset = footerBytes.length - FOOTER_SIZE;
    ByteBuffer buf = ByteBuffer.wrap(footerBytes, offset, FOOTER_SIZE)
        .order(ByteOrder.LITTLE_ENDIAN);

    long colMeta0 = buf.getLong();
    long cmoTable = buf.getLong();
    long gboTable = buf.getLong();
    int numGlobal = buf.getInt();
    int numCols = buf.getInt();
    int major = Short.toUnsignedInt(buf.getShort());
    int minor = Short.toUnsignedInt(buf.getShort());
    byte[] magic = new byte[4];
    buf.get(magic);

    if (magic[0] != 'L' || magic[1] != 'A' || magic[2] != 'N' || magic[3] != 'C') {
      throw new IOException("Invalid magic: not a Lance file");
    }

    return new LanceFooter(colMeta0, cmoTable, gboTable, numGlobal, numCols, major, minor);
  }

  public long getColumnMeta0Offset() { return columnMeta0Offset; }
  public long getCmoTableOffset() { return cmoTableOffset; }
  public long getGboTableOffset() { return gboTableOffset; }
  public int getNumGlobalBuffers() { return numGlobalBuffers; }
  public int getNumColumns() { return numColumns; }
  public int getMajorVersion() { return majorVersion; }
  public int getMinorVersion() { return minorVersion; }

  @Override
  public String toString() {
    return String.format("LanceFooter{v%d.%d, columns=%d, globalBufs=%d, colMeta0=%d, cmo=%d, gbo=%d}",
        majorVersion, minorVersion, numColumns, numGlobalBuffers,
        columnMeta0Offset, cmoTableOffset, gboTableOffset);
  }
}
