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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Parses the Column Metadata Offset (CMO) table from a Lance v2 file. Each entry is 16 bytes:
 * position (u64) + size (u64).
 */
public class LanceCmoTable {
  private final long[] positions;
  private final long[] sizes;

  private LanceCmoTable(long[] positions, long[] sizes) {
    this.positions = positions;
    this.sizes = sizes;
  }

  public static LanceCmoTable parse(byte[] data, int numColumns) {
    ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
    long[] positions = new long[numColumns];
    long[] sizes = new long[numColumns];
    for (int i = 0; i < numColumns; i++) {
      positions[i] = buf.getLong();
      sizes[i] = buf.getLong();
    }
    return new LanceCmoTable(positions, sizes);
  }

  public long getPosition(int columnIndex) {
    return positions[columnIndex];
  }

  public long getSize(int columnIndex) {
    return sizes[columnIndex];
  }

  public int getNumColumns() {
    return positions.length;
  }
}
