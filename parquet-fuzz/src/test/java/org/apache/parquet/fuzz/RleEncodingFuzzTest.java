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
package org.apache.parquet.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;

/**
 * Fuzz test for Run-Length Encoding / Bit-Packing Hybrid encoder and decoder.
 *
 * <p>Tests both roundtrip encoding (write then read) and raw decoding of arbitrary bytes.
 */
public class RleEncodingFuzzTest {

  private static final HeapByteBufferAllocator ALLOCATOR = new HeapByteBufferAllocator();

  /** OSS-Fuzz entry point. Multiplexes between roundtrip and raw-decode modes. */
  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    int mode = data.consumeInt(0, 1);
    RleEncodingFuzzTest instance = new RleEncodingFuzzTest();
    switch (mode) {
      case 0:
        instance.fuzzRleRoundtrip(data);
        break;
      default:
        instance.fuzzRleRawDecode(data);
        break;
    }
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzRleRoundtrip(FuzzedDataProvider data) {
    // bitWidth must be 1..32 for valid RLE encoding
    int bitWidth = data.consumeInt(1, 32);
    int valueCount = data.consumeInt(0, 500);
    int maxValue = (bitWidth < 32) ? (1 << bitWidth) - 1 : Integer.MAX_VALUE;

    try {
      RunLengthBitPackingHybridEncoder encoder =
          new RunLengthBitPackingHybridEncoder(bitWidth, 64 * 1024, 1024 * 1024, ALLOCATOR);

      int[] values = new int[valueCount];
      for (int i = 0; i < valueCount; i++) {
        values[i] = data.consumeInt(0, maxValue);
        encoder.writeInt(values[i]);
      }

      byte[] encoded = encoder.toBytes().toByteArray();
      RunLengthBitPackingHybridDecoder decoder =
          new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(encoded));

      for (int i = 0; i < valueCount; i++) {
        int read = decoder.readInt();
        if (read != values[i]) {
          throw new AssertionError(
              "RLE roundtrip mismatch at index " + i + ": expected " + values[i] + ", got " + read);
        }
      }
    } catch (IOException | IllegalArgumentException e) {
      // Expected for certain fuzz inputs
    }
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzRleRawDecode(FuzzedDataProvider data) {
    int bitWidth = data.consumeInt(1, 32);
    byte[] rawBytes = data.consumeRemainingAsBytes();
    if (rawBytes.length == 0) {
      return;
    }

    try {
      RunLengthBitPackingHybridDecoder decoder =
          new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(rawBytes));
      // Try reading up to a reasonable number of values
      int maxReads = Math.min(10000, rawBytes.length * 8);
      for (int i = 0; i < maxReads; i++) {
        decoder.readInt();
      }
    } catch (IOException | IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
      // Expected when decoding arbitrary bytes
    }
  }
}
