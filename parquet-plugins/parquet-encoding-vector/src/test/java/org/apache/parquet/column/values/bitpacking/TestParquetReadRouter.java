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
package org.apache.parquet.column.values.bitpacking;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.junit.Test;

public class TestParquetReadRouter {
  @Test
  public void readBatchMatchesScalarForArrayBackedBuffers() throws IOException {
    assertReadBatchMatchesScalar(false);
  }

  @Test
  public void readBatchMatchesScalarForDirectBuffers() throws IOException {
    assertReadBatchMatchesScalar(true);
  }

  @Test
  public void readBatchUsing512VectorMatchesScalarForExactSizeVectorInput() throws IOException {
    for (int bitWidth = 1; bitWidth <= 32; bitWidth++) {
      int currentCount = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth).getUnpackCount();
      assertVectorReadMatchesScalar(bitWidth, currentCount, false);
      assertVectorReadMatchesScalar(bitWidth, currentCount, true);
    }
  }

  @Test
  public void readBatchUsing512VectorMatchesScalarForTightTwoVectorInput() throws IOException {
    for (int bitWidth = 1; bitWidth <= 32; bitWidth++) {
      int currentCount = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth).getUnpackCount() * 2;
      assertVectorReadMatchesScalar(bitWidth, currentCount, false);
      assertVectorReadMatchesScalar(bitWidth, currentCount, true);
    }
  }

  private void assertReadBatchMatchesScalar(boolean directBuffer) throws IOException {
    for (int bitWidth = 1; bitWidth <= 32; bitWidth++) {
      int currentCount = 256;
      int[] expected = decodeScalar(bitWidth, currentCount, directBuffer);
      int[] actual = new int[currentCount];
      ParquetReadRouter.readBatch(bitWidth, newInput(bitWidth, currentCount, directBuffer), currentCount, actual);
      assertArrayEquals("bitWidth=" + bitWidth + ", directBuffer=" + directBuffer, expected, actual);
    }
  }

  private void assertVectorReadMatchesScalar(int bitWidth, int currentCount, boolean directBuffer) throws IOException {
    int[] expected = decodeScalar(bitWidth, currentCount, directBuffer);
    int[] actual = new int[currentCount];
    ParquetReadRouter.readBatchUsing512Vector(bitWidth, newInput(bitWidth, currentCount, directBuffer), currentCount, actual);
    assertArrayEquals(
        "bitWidth=" + bitWidth + ", currentCount=" + currentCount + ", directBuffer=" + directBuffer,
        expected,
        actual);
  }

  private int[] decodeScalar(int bitWidth, int currentCount, boolean directBuffer) throws IOException {
    int[] output = new int[currentCount];
    int[] packedValues = getSequentialValues(currentCount, bitWidth);
    byte[] packedBytes = pack(bitWidth, packedValues);

    ParquetReadRouter.readBatch(bitWidth, newInput(packedBytes, directBuffer), currentCount, output);
    return output;
  }

  private ByteBufferInputStream newInput(int bitWidth, int currentCount, boolean directBuffer) {
    return newInput(pack(bitWidth, getSequentialValues(currentCount, bitWidth)), directBuffer);
  }

  private ByteBufferInputStream newInput(byte[] packedBytes, boolean directBuffer) {
    ByteBuffer buffer = directBuffer ? ByteBuffer.allocateDirect(packedBytes.length) : ByteBuffer.allocate(packedBytes.length);
    buffer.put(packedBytes);
    buffer.flip();
    return ByteBufferInputStream.wrap(buffer);
  }

  private byte[] pack(int bitWidth, int[] values) {
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    byte[] packed = new byte[values.length * bitWidth / 8];
    for (int i = 0; i < values.length / 8; i++) {
      packer.pack8Values(values, i * 8, packed, i * bitWidth);
    }
    return packed;
  }

  private int[] getSequentialValues(int currentCount, int bitWidth) {
    int maxValue = bitWidth == 32 ? Integer.MAX_VALUE : (1 << bitWidth) - 1;
    int[] values = new int[currentCount];
    for (int i = 0; i < currentCount; i++) {
      values[i] = Math.min(i, maxValue);
    }
    return values;
  }
}
