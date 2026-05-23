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
package org.apache.parquet.column.values.alp;

import static org.apache.parquet.column.values.alp.AlpConstants.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.Test;

/**
 * Tests that ALP readers correctly reject corrupted/malicious data
 * with ParquetDecodingException rather than raw Java exceptions.
 */
public class AlpCorruptedDataTest {

  // ========== Helper: build a valid ALP page, then corrupt specific bytes ==========

  /**
   * Encodes a small float array and returns the raw page bytes.
   */
  private byte[] buildValidFloatPage(float[] values, int vectorSize) throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      int capacity = Math.max(256, values.length * 8);
      writer = new AlpValuesWriter.FloatAlpValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator(), vectorSize);
      for (float v : values) {
        writer.writeFloat(v);
      }
      BytesInput input = writer.getBytes();
      byte[] bytes = input.toByteArray();
      return bytes;
    } finally {
      if (writer != null) writer.close();
    }
  }

  /**
   * Encodes a small double array and returns the raw page bytes.
   */
  private byte[] buildValidDoublePage(double[] values, int vectorSize) throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      int capacity = Math.max(256, values.length * 8);
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator(), vectorSize);
      for (double v : values) {
        writer.writeDouble(v);
      }
      BytesInput input = writer.getBytes();
      return input.toByteArray();
    } finally {
      if (writer != null) writer.close();
    }
  }

  private ByteBufferInputStream wrapBytes(byte[] data) {
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(data));
  }

  // ========== Header validation tests ==========

  @Test
  public void testInvalidCompressionMode() throws Exception {
    byte[] page = buildValidFloatPage(new float[] {1.0f, 2.0f, 3.0f}, DEFAULT_VECTOR_SIZE);
    // Byte 0 is compressionMode
    page[0] = 1;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    try {
      reader.initFromPage(3, wrapBytes(page));
      fail("Expected ParquetDecodingException for invalid compression mode");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("compression mode"));
    }
  }

  @Test
  public void testInvalidIntegerEncoding() throws Exception {
    byte[] page = buildValidFloatPage(new float[] {1.0f, 2.0f, 3.0f}, DEFAULT_VECTOR_SIZE);
    // Byte 1 is integerEncoding
    page[1] = 5;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    try {
      reader.initFromPage(3, wrapBytes(page));
      fail("Expected ParquetDecodingException for invalid integer encoding");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("integer encoding"));
    }
  }

  @Test
  public void testInvalidLogVectorSizeTooSmall() throws Exception {
    byte[] page = buildValidFloatPage(new float[] {1.0f, 2.0f, 3.0f}, DEFAULT_VECTOR_SIZE);
    // Byte 2 is logVectorSize
    page[2] = 2; // below minimum of 3
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    try {
      reader.initFromPage(3, wrapBytes(page));
      fail("Expected ParquetDecodingException for invalid logVectorSize");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("log vector size"));
    }
  }

  @Test
  public void testInvalidLogVectorSizeTooLarge() throws Exception {
    byte[] page = buildValidFloatPage(new float[] {1.0f, 2.0f, 3.0f}, DEFAULT_VECTOR_SIZE);
    // Byte 2 is logVectorSize
    page[2] = 16; // above maximum of 15
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    try {
      reader.initFromPage(3, wrapBytes(page));
      fail("Expected ParquetDecodingException for invalid logVectorSize");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("log vector size"));
    }
  }

  @Test
  public void testNegativeNumElements() throws Exception {
    byte[] page = buildValidFloatPage(new float[] {1.0f, 2.0f, 3.0f}, DEFAULT_VECTOR_SIZE);
    // Bytes 3-6 are numElements (little-endian int)
    putIntLE(page, 3, -1);
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    try {
      reader.initFromPage(3, wrapBytes(page));
      fail("Expected ParquetDecodingException for negative numElements");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("element count"));
    }
  }

  // ========== V1: numElements too large for stream ==========

  @Test
  public void testNumElementsExceedsStreamSize() throws Exception {
    byte[] page = buildValidFloatPage(new float[] {1.0f, 2.0f, 3.0f}, DEFAULT_VECTOR_SIZE);
    // Set numElements to a huge value — requires massive offset array
    putIntLE(page, 3, Integer.MAX_VALUE);
    // Set logVectorSize to minimum (3 → vectorSize=8) to maximize numVectors
    page[2] = 3;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    try {
      reader.initFromPage(3, wrapBytes(page));
      fail("Expected ParquetDecodingException for numElements exceeding stream");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("exceeds available stream"));
    }
  }

  // ========== V2: corrupted vectorOffsets ==========

  @Test
  public void testCorruptedVectorOffsetNegativePosition() throws Exception {
    byte[] page = buildValidFloatPage(new float[] {1.0f, 2.0f, 3.0f}, DEFAULT_VECTOR_SIZE);
    // Offset array starts at byte 7 (after header). Set first offset to 0, which produces
    // negative position: 0 - offsetArraySize < 0
    putIntLE(page, ALP_HEADER_SIZE, 0);
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readFloat();
      fail("Expected ParquetDecodingException for negative vector position");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("vector offset out of bounds"));
    }
  }

  @Test
  public void testCorruptedVectorOffsetTooLarge() throws Exception {
    byte[] page = buildValidFloatPage(new float[] {1.0f, 2.0f, 3.0f}, DEFAULT_VECTOR_SIZE);
    // Set first offset to MAX_INT
    putIntLE(page, ALP_HEADER_SIZE, Integer.MAX_VALUE);
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readFloat();
      fail("Expected ParquetDecodingException for too-large vector offset");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("vector offset out of bounds"));
    }
  }

  // ========== V3: corrupted exponent/factor ==========

  @Test
  public void testCorruptedExponentFloat() throws Exception {
    float[] values = new float[] {1.23f, 4.56f, 7.89f};
    byte[] page = buildValidFloatPage(values, DEFAULT_VECTOR_SIZE);
    // Find vector data start: header(7) + offsetArray(4 * numVectors)
    // numVectors = 1 for 3 values with vectorSize=1024
    int vectorDataStart = ALP_HEADER_SIZE + 4; // offset array is 4 bytes (1 vector)
    // First byte of vector data after offset array is the actual vector data
    // The offset in the offset array points to the vector start relative to after header
    // We need to find where exponent is in the raw data
    // vectorOffsets[0] points to start of vector data (relative to after header)
    // pos = vectorOffsets[0] - offsetArraySize
    // In the raw page, exponent is at: headerSize + offsetArraySize + pos
    int offsetArraySize = 4; // 1 vector * 4 bytes
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    int exponentByteIdx = ALP_HEADER_SIZE + offsetArraySize + pos;
    // Set exponent to invalid value
    page[exponentByteIdx] = (byte) 200;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readFloat();
      fail("Expected ParquetDecodingException for invalid exponent");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("exponent") && e.getMessage().contains("FLOAT"));
    }
  }

  @Test
  public void testCorruptedFactorFloat() throws Exception {
    float[] values = new float[] {1.23f, 4.56f, 7.89f};
    byte[] page = buildValidFloatPage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    int factorByteIdx = ALP_HEADER_SIZE + offsetArraySize + pos + 1;
    page[factorByteIdx] = (byte) 200;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readFloat();
      fail("Expected ParquetDecodingException for invalid factor");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("factor") && e.getMessage().contains("FLOAT"));
    }
  }

  @Test
  public void testCorruptedExponentDouble() throws Exception {
    double[] values = new double[] {1.23, 4.56, 7.89};
    byte[] page = buildValidDoublePage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    int exponentByteIdx = ALP_HEADER_SIZE + offsetArraySize + pos;
    page[exponentByteIdx] = (byte) 200;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readDouble();
      fail("Expected ParquetDecodingException for invalid exponent");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("exponent") && e.getMessage().contains("DOUBLE"));
    }
  }

  @Test
  public void testCorruptedFactorDouble() throws Exception {
    double[] values = new double[] {1.23, 4.56, 7.89};
    byte[] page = buildValidDoublePage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    int factorByteIdx = ALP_HEADER_SIZE + offsetArraySize + pos + 1;
    page[factorByteIdx] = (byte) 200;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readDouble();
      fail("Expected ParquetDecodingException for invalid factor");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("factor") && e.getMessage().contains("DOUBLE"));
    }
  }

  // ========== V4: corrupted bitWidth ==========

  @Test
  public void testCorruptedBitWidthFloat() throws Exception {
    float[] values = new float[] {1.23f, 4.56f, 7.89f};
    byte[] page = buildValidFloatPage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    // bitWidth is at ALP_INFO_SIZE(4) + 4 (frameOfReference for float) = offset +8 from vector start
    int bitWidthIdx = ALP_HEADER_SIZE + offsetArraySize + pos + ALP_INFO_SIZE + 4;
    page[bitWidthIdx] = (byte) 255;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readFloat();
      fail("Expected ParquetDecodingException for invalid bit width");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("bit width") && e.getMessage().contains("FLOAT"));
    }
  }

  @Test
  public void testCorruptedBitWidthDouble() throws Exception {
    double[] values = new double[] {1.23, 4.56, 7.89};
    byte[] page = buildValidDoublePage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    // bitWidth for double is at ALP_INFO_SIZE(4) + 8 (frameOfReference for double) = offset +12
    int bitWidthIdx = ALP_HEADER_SIZE + offsetArraySize + pos + ALP_INFO_SIZE + 8;
    page[bitWidthIdx] = (byte) 255;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readDouble();
      fail("Expected ParquetDecodingException for invalid bit width");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("bit width") && e.getMessage().contains("DOUBLE"));
    }
  }

  // ========== V5: corrupted numExceptions ==========

  @Test
  public void testCorruptedNumExceptionsFloat() throws Exception {
    float[] values = new float[] {1.23f, 4.56f, 7.89f};
    byte[] page = buildValidFloatPage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    // numExceptions is at bytes 2-3 of vector data (little-endian uint16)
    int numExcIdx = ALP_HEADER_SIZE + offsetArraySize + pos + 2;
    // Set numExceptions to 65535 (way larger than vectorLen=3)
    page[numExcIdx] = (byte) 0xFF;
    page[numExcIdx + 1] = (byte) 0xFF;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readFloat();
      fail("Expected ParquetDecodingException for numExceptions > vectorLen");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("numExceptions"));
    }
  }

  @Test
  public void testCorruptedNumExceptionsDouble() throws Exception {
    double[] values = new double[] {1.23, 4.56, 7.89};
    byte[] page = buildValidDoublePage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    int numExcIdx = ALP_HEADER_SIZE + offsetArraySize + pos + 2;
    page[numExcIdx] = (byte) 0xFF;
    page[numExcIdx + 1] = (byte) 0xFF;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(3, wrapBytes(page));
    try {
      reader.readDouble();
      fail("Expected ParquetDecodingException for numExceptions > vectorLen");
    } catch (ParquetDecodingException e) {
      assertTrue(e.getMessage().contains("numExceptions"));
    }
  }

  // ========== V6: corrupted exception positions ==========

  @Test
  public void testCorruptedExceptionPositionFloat() throws Exception {
    // Use values that will generate exceptions (NaN, Inf)
    float[] values = new float[] {1.23f, Float.NaN, 3.45f, Float.POSITIVE_INFINITY, 5.67f};
    byte[] page = buildValidFloatPage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    int numExceptions = getShortLE(page, ALP_HEADER_SIZE + offsetArraySize + pos + 2);

    if (numExceptions > 0) {
      // Exception positions start after ALP_INFO_SIZE + FLOAT_FOR_INFO_SIZE + bit-packed data
      // We need to find where the exception positions are
      // Simpler approach: corrupt the first exception position byte to an out-of-range value
      int bitWidth = page[ALP_HEADER_SIZE + offsetArraySize + pos + ALP_INFO_SIZE + 4] & 0xFF;
      int vectorLen = 5;
      int packedBytesSize = (bitWidth > 0) ? (vectorLen * bitWidth + 7) / 8 : 0;
      int excPosStart =
          ALP_HEADER_SIZE + offsetArraySize + pos + ALP_INFO_SIZE + FLOAT_FOR_INFO_SIZE + packedBytesSize;
      // Set first exception position to 60000 (way beyond vectorLen=5)
      page[excPosStart] = (byte) 0x60;
      page[excPosStart + 1] = (byte) 0xEA; // 60000 = 0xEA60 in LE

      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(5, wrapBytes(page));
      try {
        reader.readFloat();
        fail("Expected ParquetDecodingException for out-of-range exception position");
      } catch (ParquetDecodingException e) {
        assertTrue(e.getMessage().contains("exception position"));
      }
    }
  }

  @Test
  public void testCorruptedExceptionPositionDouble() throws Exception {
    double[] values = new double[] {1.23, Double.NaN, 3.45, Double.POSITIVE_INFINITY, 5.67};
    byte[] page = buildValidDoublePage(values, DEFAULT_VECTOR_SIZE);
    int offsetArraySize = 4;
    int vectorOffset = getIntLE(page, ALP_HEADER_SIZE);
    int pos = vectorOffset - offsetArraySize;
    int numExceptions = getShortLE(page, ALP_HEADER_SIZE + offsetArraySize + pos + 2);

    if (numExceptions > 0) {
      int bitWidth = page[ALP_HEADER_SIZE + offsetArraySize + pos + ALP_INFO_SIZE + 8] & 0xFF;
      int vectorLen = 5;
      int packedBytesSize = (bitWidth > 0) ? (vectorLen * bitWidth + 7) / 8 : 0;
      int excPosStart =
          ALP_HEADER_SIZE + offsetArraySize + pos + ALP_INFO_SIZE + DOUBLE_FOR_INFO_SIZE + packedBytesSize;
      // Set first exception position to 60000 (way beyond vectorLen=5)
      page[excPosStart] = (byte) 0x60;
      page[excPosStart + 1] = (byte) 0xEA; // 60000 = 0xEA60 in LE

      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(5, wrapBytes(page));
      try {
        reader.readDouble();
        fail("Expected ParquetDecodingException for out-of-range exception position");
      } catch (ParquetDecodingException e) {
        assertTrue(e.getMessage().contains("exception position"));
      }
    }
  }

  // ========== Boundary tests: valid edge cases still work ==========

  @Test
  public void testMaxValidExponentFloat() throws Exception {
    // exponent=10 (max) should still work
    float[] values = new float[] {1.23f, 4.56f, 7.89f};
    byte[] page = buildValidFloatPage(values, DEFAULT_VECTOR_SIZE);
    // Verify we can still read valid data without issues
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(3, wrapBytes(page));
    for (float expected : values) {
      assertEquals(expected, reader.readFloat(), 0.0f);
    }
  }

  @Test
  public void testMaxValidExponentDouble() throws Exception {
    double[] values = new double[] {1.23, 4.56, 7.89};
    byte[] page = buildValidDoublePage(values, DEFAULT_VECTOR_SIZE);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(3, wrapBytes(page));
    for (double expected : values) {
      assertEquals(expected, reader.readDouble(), 0.0);
    }
  }

  // ========== Utility methods ==========

  private static void putIntLE(byte[] buf, int offset, int value) {
    buf[offset] = (byte) (value & 0xFF);
    buf[offset + 1] = (byte) ((value >>> 8) & 0xFF);
    buf[offset + 2] = (byte) ((value >>> 16) & 0xFF);
    buf[offset + 3] = (byte) ((value >>> 24) & 0xFF);
  }

  private static int getIntLE(byte[] buf, int offset) {
    return (buf[offset] & 0xFF)
        | ((buf[offset + 1] & 0xFF) << 8)
        | ((buf[offset + 2] & 0xFF) << 16)
        | ((buf[offset + 3] & 0xFF) << 24);
  }

  private static int getShortLE(byte[] buf, int offset) {
    return (buf[offset] & 0xFF) | ((buf[offset + 1] & 0xFF) << 8);
  }
}
