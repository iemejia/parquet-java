/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.rle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 */
public class RunLengthBitPackingHybridDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(RunLengthBitPackingHybridDecoder.class);

  private static enum MODE {
    RLE,
    PACKED
  }

  private final int bitWidth;
  private final BytePacker packer;
  private final ByteBuffer buffer;

  private MODE mode;
  private int currentCount;
  private int currentValue;
  private int[] currentBuffer;
  private int currentBufferIdx;

  // Reusable buffers to avoid per-run allocation in PACKED mode
  private int[] packedValuesBuffer = new int[0];
  private byte[] packedBytesBuffer = new byte[0];

  public RunLengthBitPackingHybridDecoder(int bitWidth, ByteBuffer buffer) {
    LOG.debug("decoding bitWidth {}", bitWidth);

    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Reads the next int value from the RLE/Bit-Packing hybrid stream.
   *
   * @return the next decoded integer value
   * @throws ParquetDecodingException if a decoding error occurs
   */
  public int readInt() {
    if (currentCount == 0) {
      readNext();
    }
    --currentCount;
    switch (mode) {
      case RLE:
        return currentValue;
      case PACKED:
        return currentBuffer[currentBufferIdx++];
      default:
        throw new ParquetDecodingException("not a valid mode " + mode);
    }
  }

  /**
   * Reads {@code count} int values into {@code dest} starting at {@code offset}.
   * This avoids per-value virtual dispatch overhead.
   *
   * @param dest   destination array
   * @param offset start index in dest
   * @param count  number of values to read
   */
  public void readInts(int[] dest, int offset, int count) {
    int remaining = count;
    int pos = offset;
    while (remaining > 0) {
      if (currentCount == 0) {
        readNext();
      }
      int batchSize = Math.min(remaining, currentCount);
      switch (mode) {
        case RLE:
          java.util.Arrays.fill(dest, pos, pos + batchSize, currentValue);
          break;
        case PACKED:
          System.arraycopy(currentBuffer, currentBufferIdx, dest, pos, batchSize);
          currentBufferIdx += batchSize;
          break;
        default:
          throw new ParquetDecodingException("not a valid mode " + mode);
      }
      currentCount -= batchSize;
      remaining -= batchSize;
      pos += batchSize;
    }
  }

  private void readNext() {
    Preconditions.checkArgument(buffer.hasRemaining(), "Reading past RLE/BitPacking stream.");
    final int header = BytesUtils.readUnsignedVarInt(buffer);
    mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
    switch (mode) {
      case RLE:
        currentCount = header >>> 1;
        LOG.debug("reading {} values RLE", currentCount);
        currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(buffer, bitWidth);
        break;
      case PACKED:
        int numGroups = header >>> 1;
        currentCount = numGroups * 8;
        currentBufferIdx = 0;
        LOG.debug("reading {} values BIT PACKED", currentCount);
        if (packedValuesBuffer.length < currentCount) {
          packedValuesBuffer = new int[currentCount];
        }
        currentBuffer = packedValuesBuffer;
        int bytesRequired = numGroups * bitWidth;
        if (packedBytesBuffer.length < bytesRequired) {
          packedBytesBuffer = new byte[bytesRequired];
        }
        // At the end of the file RLE data though, there might not be that many bytes left.
        int bytesToRead = (int) Math.ceil(currentCount * bitWidth / 8.0);
        bytesToRead = Math.min(bytesToRead, buffer.remaining());
        buffer.get(packedBytesBuffer, 0, bytesToRead);
        // Unpack 32 values (4 groups) at a time when possible — symmetric to the encoder's
        // pack32Values fast path. Falls back to unpack8Values for any residual groups.
        int groupIdx = 0;
        int byteIndex = 0;
        final int step32 = bitWidth * 4;
        while (groupIdx + 4 <= numGroups) {
          packer.unpack32Values(packedBytesBuffer, byteIndex, currentBuffer, groupIdx * 8);
          groupIdx += 4;
          byteIndex += step32;
        }
        for (; groupIdx < numGroups; groupIdx++, byteIndex += bitWidth) {
          packer.unpack8Values(packedBytesBuffer, byteIndex, currentBuffer, groupIdx * 8);
        }
        break;
      default:
        throw new ParquetDecodingException("not a valid mode " + mode);
    }
  }
}
