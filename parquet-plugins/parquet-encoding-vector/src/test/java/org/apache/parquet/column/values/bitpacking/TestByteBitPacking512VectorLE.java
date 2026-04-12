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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Assume;
import org.junit.Test;

public class TestByteBitPacking512VectorLE {
  @Test
  public void unpackValuesUsingVector() {
    Assume.assumeTrue(ParquetReadRouter.getSupportVectorFromCPUFlags() == VectorSupport.VECTOR_512);
    for (int i = 1; i <= 32; i++) {
      unpackValuesUsingVectorBitWidth(i);
    }
  }

  @Test
  public void unpackValuesUsingVectorExactSizeArray() {
    assertExactSizeVectorRoundTrip(this::vectorCallIsExactSizeSafe, this::unpackSingleVectorArray);
  }

  @Test
  public void unpackValuesUsingVectorExactSizeByteBuffer() {
    assertExactSizeVectorRoundTrip(this::vectorCallIsExactSizeSafe, this::unpackSingleVectorByteBuffer);
  }

  private void unpackValuesUsingVectorBitWidth(int bitWidth) {
    try (Stream<int[]> intInputs = getRangeData(bitWidth)) {
      intInputs.forEach(intInput -> {
        int pack8Count = intInput.length / 8;
        int byteOutputSize = pack8Count * bitWidth;
        byte[] byteOutput = new byte[byteOutputSize];
        int[] output = new int[intInput.length];

        BytePacker bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        for (int i = 0; i < pack8Count; i++) {
          bytePacker.pack8Values(intInput, 8 * i, byteOutput, bitWidth * i);
        }

        unpack8Values(bitWidth, byteOutput, output);
        assertArrayEquals(intInput, output);
        Arrays.fill(output, 0);

        unpackValuesUsingVectorArray(bitWidth, byteOutput, output);
        assertArrayEquals(intInput, output);
        Arrays.fill(output, 0);

        ByteBuffer byteBuffer = ByteBuffer.wrap(byteOutput);
        unpackValuesUsingVectorByteBuffer(bitWidth, byteBuffer, output);
        assertArrayEquals(intInput, output);
        Arrays.fill(output, 0);
      });
    }
  }

  public void unpack8Values(int bitWidth, byte[] input, int[] output) {
    BytePacker bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int len = input.length;
    int i = 0, j = 0;
    for (; i < len; i += bitWidth, j += 8) {
      bytePacker.unpack8Values(input, i, output, j);
    }
  }

  public void unpackValuesUsingVectorArray(int bitWidth, byte[] input, int[] output) {
    BytePacker bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    BytePacker bytePackerVector = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);

    int byteIndex = 0;
    int valueIndex = 0;
    int totalByteCount = input.length;
    int outCountPerVector = bytePackerVector.getUnpackCount();
    int inputByteCountPerVector = outCountPerVector / 8 * bitWidth;
    int totalByteCountVector = totalByteCount - inputByteCountPerVector;

    for (;
        byteIndex < totalByteCountVector;
        byteIndex += inputByteCountPerVector, valueIndex += outCountPerVector) {
      bytePackerVector.unpackValuesUsingVector(input, byteIndex, output, valueIndex);
    }
    for (; byteIndex < totalByteCount; byteIndex += bitWidth, valueIndex += 8) {
      bytePacker.unpack8Values(input, byteIndex, output, valueIndex);
    }
  }

  public void unpackValuesUsingVectorByteBuffer(int bitWidth, ByteBuffer input, int[] output) {
    BytePacker bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    BytePacker bytePackerVector = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);

    int byteIndex = 0;
    int valueIndex = 0;
    int totalByteCount = input.capacity();
    int outCountPerVector = bytePackerVector.getUnpackCount();
    int inputByteCountPerVector = outCountPerVector / 8 * bitWidth;
    int totalByteCountVector = totalByteCount - inputByteCountPerVector;

    for (;
        byteIndex < totalByteCountVector;
        byteIndex += inputByteCountPerVector, valueIndex += outCountPerVector) {
      bytePackerVector.unpackValuesUsingVector(input, byteIndex, output, valueIndex);
    }
    for (; byteIndex < totalByteCount; byteIndex += bitWidth, valueIndex += 8) {
      bytePacker.unpack8Values(input, byteIndex, output, valueIndex);
    }
  }

  private void unpackSingleVectorArray(int bitWidth, byte[] input, int[] output) {
    Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth).unpackValuesUsingVector(input, 0, output, 0);
  }

  private void unpackSingleVectorByteBuffer(int bitWidth, byte[] input, int[] output) {
    Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth).unpackValuesUsingVector(ByteBuffer.wrap(input), 0, output, 0);
  }

  private void assertExactSizeVectorRoundTrip(IntPredicate supportedBitWidth, VectorUnpacker unpacker) {
    for (int bitWidth = 1; bitWidth <= 32; bitWidth++) {
      if (!supportedBitWidth.test(bitWidth)) {
        continue;
      }

      BytePacker scalarPacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
      BytePacker vectorPacker = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);
      int unpackCount = vectorPacker.getUnpackCount();
      int byteCount = unpackCount / 8 * bitWidth;
      int[] input = getSequentialValues(unpackCount, bitWidth);
      byte[] packed = new byte[byteCount];
      int[] output = new int[unpackCount];

      for (int i = 0; i < unpackCount / 8; i++) {
        scalarPacker.pack8Values(input, i * 8, packed, i * bitWidth);
      }

      unpacker.unpack(bitWidth, packed, output);
      assertArrayEquals("bitWidth=" + bitWidth, input, output);
    }
  }

  private boolean vectorCallIsExactSizeSafe(int bitWidth) {
    return bitWidth != 1 && bitWidth != 2;
  }

  private int[] getSequentialValues(int length, int bitWidth) {
    int maxValue = bitWidth == 32 ? Integer.MAX_VALUE : (1 << bitWidth) - 1;
    int[] values = new int[length];
    for (int i = 0; i < length; i++) {
      values[i] = Math.min(i, maxValue);
    }
    return values;
  }

  @FunctionalInterface
  private interface VectorUnpacker {
    void unpack(int bitWidth, byte[] input, int[] output);
  }

  private Stream<int[]> getRangeData(int bitWidth) {
    int itemMax = 268435456;

    long maxValue = getMaxValue(bitWidth);
    long maxValueFilled = maxValue + 1;
    int itemCount = (int) (maxValueFilled / itemMax);
    int mode = (int) (maxValueFilled % itemMax);
    if (mode != 0) {
      ++itemCount;
    }

    final int finalItemCount = itemCount;

    return IntStream.range(0, finalItemCount).mapToObj(i -> {
      int len;
      if ((i == finalItemCount - 1) && mode != 0) {
        len = mode;
      } else {
        len = itemMax;
      }
      if (len < 64) {
        len = 64;
      } else {
        len += 64;
      }
      int[] array = new int[len];
      int j = 0;
      while (j < len) {
        int value = j + i * itemMax;
        if (value > maxValue) {
          if (maxValue < Integer.MAX_VALUE) {
            value = (int) maxValue;
          } else {
            value = Integer.MAX_VALUE;
          }
        }
        if (value < 0) {
          if (bitWidth < 32) {
            value = value - Integer.MIN_VALUE;
          }
        }
        array[j] = value;
        j++;
      }
      return array;
    });
  }

  private long getMaxValue(int bitWidth) {
    return BigDecimal.valueOf(Math.pow(2, bitWidth)).longValue() - 1;
  }
}
