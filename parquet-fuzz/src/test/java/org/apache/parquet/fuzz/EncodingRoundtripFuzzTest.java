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
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForDouble;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForFloat;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForInteger;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForLong;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.PlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;

/**
 * Fuzz test for encoding writer/reader roundtrips.
 *
 * <p>Uses {@link FuzzedDataProvider} to select an encoding type and generate values,
 * then writes values through the encoder and reads them back through the decoder,
 * verifying that the roundtrip produces identical results.
 */
public class EncodingRoundtripFuzzTest {

  private static final int SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 1024 * 1024;
  private static final HeapByteBufferAllocator ALLOCATOR = new HeapByteBufferAllocator();

  private enum EncodingType {
    PLAIN_INT,
    PLAIN_LONG,
    PLAIN_FLOAT,
    PLAIN_DOUBLE,
    DELTA_BINARY_PACKED_INT,
    DELTA_BINARY_PACKED_LONG,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY,
    BYTE_STREAM_SPLIT_FLOAT,
    BYTE_STREAM_SPLIT_DOUBLE,
    BYTE_STREAM_SPLIT_INT,
    BYTE_STREAM_SPLIT_LONG
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzEncodingRoundtrip(FuzzedDataProvider data) {
    EncodingType encoding = data.pickValue(EncodingType.values());
    int valueCount = data.consumeInt(0, 500);

    try {
      switch (encoding) {
        case PLAIN_INT:
          testPlainIntRoundtrip(data, valueCount);
          break;
        case PLAIN_LONG:
          testPlainLongRoundtrip(data, valueCount);
          break;
        case PLAIN_FLOAT:
          testPlainFloatRoundtrip(data, valueCount);
          break;
        case PLAIN_DOUBLE:
          testPlainDoubleRoundtrip(data, valueCount);
          break;
        case DELTA_BINARY_PACKED_INT:
          testDeltaIntRoundtrip(data, valueCount);
          break;
        case DELTA_BINARY_PACKED_LONG:
          testDeltaLongRoundtrip(data, valueCount);
          break;
        case DELTA_LENGTH_BYTE_ARRAY:
          testDeltaLengthByteArrayRoundtrip(data, valueCount);
          break;
        case DELTA_BYTE_ARRAY:
          testDeltaByteArrayRoundtrip(data, valueCount);
          break;
        case BYTE_STREAM_SPLIT_FLOAT:
          testByteStreamSplitFloatRoundtrip(data, valueCount);
          break;
        case BYTE_STREAM_SPLIT_DOUBLE:
          testByteStreamSplitDoubleRoundtrip(data, valueCount);
          break;
        case BYTE_STREAM_SPLIT_INT:
          testByteStreamSplitIntRoundtrip(data, valueCount);
          break;
        case BYTE_STREAM_SPLIT_LONG:
          testByteStreamSplitLongRoundtrip(data, valueCount);
          break;
      }
    } catch (IOException | IllegalArgumentException | IllegalStateException e) {
      // Expected for malformed fuzz inputs
    }
  }

  private void testPlainIntRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    PlainValuesWriter writer = new PlainValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeInt();
      writer.writeInteger(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    ValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      int read = reader.readInteger();
      if (read != values[i]) {
        throw new AssertionError("Plain int mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testPlainLongRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    PlainValuesWriter writer = new PlainValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeLong();
      writer.writeLong(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    ValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      long read = reader.readLong();
      if (read != values[i]) {
        throw new AssertionError("Plain long mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testPlainFloatRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    PlainValuesWriter writer = new PlainValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    float[] values = new float[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeFloat();
      writer.writeFloat(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    ValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      float read = reader.readFloat();
      if (Float.floatToRawIntBits(read) != Float.floatToRawIntBits(values[i])) {
        throw new AssertionError(
            "Plain float mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testPlainDoubleRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    PlainValuesWriter writer = new PlainValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeDouble();
      writer.writeDouble(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    ValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      double read = reader.readDouble();
      if (Double.doubleToRawLongBits(read) != Double.doubleToRawLongBits(values[i])) {
        throw new AssertionError(
            "Plain double mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testDeltaIntRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    ValuesWriter writer = new DeltaBinaryPackingValuesWriterForInteger(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeInt();
      writer.writeInteger(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      int read = reader.readInteger();
      if (read != values[i]) {
        throw new AssertionError("Delta int mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testDeltaLongRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    ValuesWriter writer = new DeltaBinaryPackingValuesWriterForLong(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeLong();
      writer.writeLong(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      long read = reader.readLong();
      if (read != values[i]) {
        throw new AssertionError("Delta long mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testDeltaLengthByteArrayRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    DeltaLengthByteArrayValuesWriter writer =
        new DeltaLengthByteArrayValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    Binary[] values = new Binary[count];
    for (int i = 0; i < count; i++) {
      byte[] bytes = data.consumeBytes(data.consumeInt(0, 100));
      values[i] = Binary.fromConstantByteArray(bytes);
      writer.writeBytes(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      Binary read = reader.readBytes();
      if (!read.equals(values[i])) {
        throw new AssertionError("DeltaLengthByteArray mismatch at index " + i);
      }
    }
  }

  private void testDeltaByteArrayRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    Binary[] values = new Binary[count];
    for (int i = 0; i < count; i++) {
      byte[] bytes = data.consumeBytes(data.consumeInt(0, 100));
      values[i] = Binary.fromConstantByteArray(bytes);
      writer.writeBytes(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      Binary read = reader.readBytes();
      if (!read.equals(values[i])) {
        throw new AssertionError("DeltaByteArray mismatch at index " + i);
      }
    }
  }

  private void testByteStreamSplitFloatRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    ValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    float[] values = new float[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeFloat();
      writer.writeFloat(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      float read = reader.readFloat();
      if (Float.floatToRawIntBits(read) != Float.floatToRawIntBits(values[i])) {
        throw new AssertionError(
            "ByteStreamSplit float mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testByteStreamSplitDoubleRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    ValuesWriter writer =
        new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeDouble();
      writer.writeDouble(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    ByteStreamSplitValuesReaderForDouble reader = new ByteStreamSplitValuesReaderForDouble();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      double read = reader.readDouble();
      if (Double.doubleToRawLongBits(read) != Double.doubleToRawLongBits(values[i])) {
        throw new AssertionError(
            "ByteStreamSplit double mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testByteStreamSplitIntRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    ValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeInt();
      writer.writeInteger(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      int read = reader.readInteger();
      if (read != values[i]) {
        throw new AssertionError(
            "ByteStreamSplit int mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }

  private void testByteStreamSplitLongRoundtrip(FuzzedDataProvider data, int count) throws IOException {
    ValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(SLAB_SIZE, PAGE_SIZE, ALLOCATOR);
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = data.consumeLong();
      writer.writeLong(values[i]);
    }
    byte[] encoded = writer.getBytes().toByteArray();
    ByteStreamSplitValuesReaderForLong reader = new ByteStreamSplitValuesReaderForLong();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < count; i++) {
      long read = reader.readLong();
      if (read != values[i]) {
        throw new AssertionError(
            "ByteStreamSplit long mismatch at index " + i + ": expected " + values[i] + ", got " + read);
      }
    }
  }
}
