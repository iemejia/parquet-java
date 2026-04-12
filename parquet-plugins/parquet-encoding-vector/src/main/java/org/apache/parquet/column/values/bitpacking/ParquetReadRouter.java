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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for big data applications (such as Apache Spark and Apache Flink).
 * For Intel CPU, Flags containing avx512vbmi and avx512_vbmi2 can have better performance gains.
 */
public class ParquetReadRouter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetReadRouter.class);

  private static final int BITS_PER_BYTE = 8;

  // values are bit packed 8 at a time, so reading bitWidth will always work
  private static final int NUM_VALUES_TO_PACK = 8;

  private static final VectorSupport vectorSupport;
  private static final BytePacker[] SCALAR_PACKERS = new BytePacker[33];
  private static final BytePacker[] VECTOR_PACKERS = new BytePacker[33];
  private static final int[] VECTOR_STEP_BYTES = new int[33];
  private static final int[] VECTOR_REQUIRED_BYTES = new int[33];

  static {
    vectorSupport = getSupportVectorFromCPUFlags();
    for (int bitWidth = 0; bitWidth < SCALAR_PACKERS.length; bitWidth++) {
      SCALAR_PACKERS[bitWidth] = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);

      BytePacker vectorPacker = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);
      VECTOR_PACKERS[bitWidth] = vectorPacker;

      int stepBytes = vectorPacker.getUnpackCount() / BITS_PER_BYTE * bitWidth;
      VECTOR_STEP_BYTES[bitWidth] = stepBytes;
      VECTOR_REQUIRED_BYTES[bitWidth] = requiredBytesPerVectorCall(bitWidth, stepBytes);
    }
  }

  // Dispatches to use vector when available. Directly call readBatchUsing512Vector() if you are sure about it.
  public static void read(int bitWidth, ByteBufferInputStream in, int currentCount, int[] currentBuffer)
      throws IOException {
    switch (vectorSupport) {
      case VECTOR_512:
        readBatchUsing512Vector(bitWidth, in, currentCount, currentBuffer);
        break;
      default:
        readBatch(bitWidth, in, currentCount, currentBuffer);
    }
  }

  // Call the method directly if your computer system contains avx512vbmi and avx512_vbmi2 CPU Flags
  public static void readBatchUsing512Vector(
      int bitWidth, ByteBufferInputStream in, int currentCount, int[] currentBuffer) throws IOException {
    BytePacker packer = SCALAR_PACKERS[bitWidth];
    BytePacker packerVector = VECTOR_PACKERS[bitWidth];
    int valueIndex = 0;
    int byteIndex = 0;
    int unpackCount = packerVector.getUnpackCount();
    int inputByteCountPerVector = VECTOR_STEP_BYTES[bitWidth];
    int requiredInputBytesPerVector = VECTOR_REQUIRED_BYTES[bitWidth];
    int totalByteCount = currentCount * bitWidth / BITS_PER_BYTE;
    ByteBuffer buffer = in.slice(totalByteCount);
    byte[] inputBytes;
    int inputOffset;
    if (buffer.hasArray()) {
      inputBytes = buffer.array();
      inputOffset = buffer.arrayOffset() + buffer.position();
    } else {
      inputBytes = new byte[totalByteCount];
      buffer.duplicate().get(inputBytes);
      inputOffset = 0;
    }

    for (; byteIndex + requiredInputBytesPerVector <= totalByteCount;
        byteIndex += inputByteCountPerVector, valueIndex += unpackCount) {
      packerVector.unpackValuesUsingVector(inputBytes, inputOffset + byteIndex, currentBuffer, valueIndex);
    }
    for (; byteIndex < totalByteCount; byteIndex += bitWidth, valueIndex += NUM_VALUES_TO_PACK) {
      packer.unpack8Values(inputBytes, inputOffset + byteIndex, currentBuffer, valueIndex);
    }
  }

  // Call the method directly if your computer system doesn't contain avx512vbmi and avx512_vbmi2 CPU Flags
  public static void readBatch(int bitWidth, ByteBufferInputStream in, int currentCount, int[] currentBuffer)
      throws EOFException {
    BytePacker packer = SCALAR_PACKERS[bitWidth];
    int valueIndex = 0;
    while (valueIndex < currentCount) {
      ByteBuffer buffer = in.slice(bitWidth);
      packer.unpack8Values(buffer, buffer.position(), currentBuffer, valueIndex);
      valueIndex += NUM_VALUES_TO_PACK;
    }
  }

  // Most kernels consume exactly one logical vector step, but bit widths 1 and 2
  // currently use 64-byte loads and need that much input available.
  private static int requiredBytesPerVectorCall(int bitWidth, int stepBytes) {
    switch (bitWidth) {
      case 1:
      case 2:
        return 64;
      default:
        return stepBytes;
    }
  }

  public static VectorSupport getSupportVectorFromCPUFlags() {
    try {
      String os = System.getProperty("os.name");
      if (os == null || !os.toLowerCase().startsWith("linux")) {
        return VectorSupport.NONE;
      }
      List<String> allLines = Files.readAllLines(Paths.get("/proc/cpuinfo"), StandardCharsets.UTF_8);
      for (String line : allLines) {
        if (line != null && line.startsWith("flags")) {
          int index = line.indexOf(":");
          if (index < 0) {
            continue;
          }
          line = line.substring(index + 1);
          Set<String> flagsSet = Arrays.stream(line.split(" ")).collect(Collectors.toSet());
          if (flagsSet.contains("avx512vbmi") && flagsSet.contains("avx512_vbmi2")) {
            return VectorSupport.VECTOR_512;
          }
        }
      }
    } catch (Exception ex) {
      LOG.warn("Failed to get CPU info");
    }
    return VectorSupport.NONE;
  }
}
