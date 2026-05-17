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
package org.apache.parquet.column.values.bytestreamsplit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

/**
 * Tests for batch read/write APIs, internal batching logic, decodeData transpose
 * specializations, advanceByteOffset, and edge cases added as part of the
 * BYTE_STREAM_SPLIT performance optimizations.
 */
public class ByteStreamSplitBatchTest {

  private static final int BATCH_SIZE = 64; // matches ByteStreamSplitValuesWriter.BATCH_SIZE

  // ---------------------------------------------------------------------------
  // Batch round-trip: batch-write -> encode -> decode -> batch-read for all 5 types
  // ---------------------------------------------------------------------------

  @Test
  public void testFloatBatchRoundTrip() throws Exception {
    Random rand = new Random(42);
    final int numElements = 1024;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; i++) {
      values[i] = rand.nextFloat() * 4096.0f - 2048.0f;
    }

    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeFloats(values, 0, numElements);

    assertEquals(numElements * 4, writer.getBufferedSize());
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    float[] dest = new float[numElements];
    reader.readFloats(dest, 0, numElements);
    assertArrayEquals(values, dest, 0.0f);

    writer.reset();
    writer.close();
  }

  @Test
  public void testDoubleBatchRoundTrip() throws Exception {
    Random rand = new Random(42);
    final int numElements = 1024;
    double[] values = rand.doubles(numElements).toArray();

    ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    writer.writeDoubles(values, 0, numElements);

    assertEquals(numElements * 8, writer.getBufferedSize());
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForDouble reader = new ByteStreamSplitValuesReaderForDouble();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    double[] dest = new double[numElements];
    reader.readDoubles(dest, 0, numElements);
    assertArrayEquals(values, dest, 0.0);

    writer.reset();
    writer.close();
  }

  @Test
  public void testIntegerBatchRoundTrip() throws Exception {
    Random rand = new Random(42);
    final int numElements = 1024;
    int[] values = rand.ints(numElements).toArray();

    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeIntegers(values, 0, numElements);

    assertEquals(numElements * 4, writer.getBufferedSize());
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    int[] dest = new int[numElements];
    reader.readIntegers(dest, 0, numElements);
    assertArrayEquals(values, dest);

    writer.reset();
    writer.close();
  }

  @Test
  public void testLongBatchRoundTrip() throws Exception {
    Random rand = new Random(42);
    final int numElements = 1024;
    long[] values = rand.longs(numElements).toArray();

    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    writer.writeLongs(values, 0, numElements);

    assertEquals(numElements * 8, writer.getBufferedSize());
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForLong reader = new ByteStreamSplitValuesReaderForLong();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    long[] dest = new long[numElements];
    reader.readLongs(dest, 0, numElements);
    assertArrayEquals(values, dest);

    writer.reset();
    writer.close();
  }

  @Test
  public void testFlbaBatchRoundTrip() throws Exception {
    Random rand = new Random(42);
    final int numElements = 1024;
    final int typeLength = 3;
    Binary[] values = new Binary[numElements];
    for (int i = 0; i < numElements; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      values[i] = Binary.fromConstantByteArray(bytes);
    }

    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength,
            numElements * typeLength,
            numElements * typeLength,
            new DirectByteBufferAllocator());
    writer.writeBinaries(values, 0, numElements);

    assertEquals(numElements * typeLength, writer.getBufferedSize());
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(typeLength);
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    Binary[] dest = new Binary[numElements];
    reader.readBinaries(dest, 0, numElements);
    for (int i = 0; i < numElements; i++) {
      assertEquals(values[i], dest[i]);
    }

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // Cross-mode: scalar-write -> batch-read, and batch-write -> scalar-read
  // ---------------------------------------------------------------------------

  @Test
  public void testScalarWriteBatchReadFloat() throws Exception {
    Random rand = new Random(99);
    final int numElements = 256;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; i++) {
      values[i] = rand.nextFloat();
    }

    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    for (float v : values) {
      writer.writeFloat(v);
    }
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    float[] dest = new float[numElements];
    reader.readFloats(dest, 0, numElements);
    assertArrayEquals(values, dest, 0.0f);

    writer.reset();
    writer.close();
  }

  @Test
  public void testBatchWriteScalarReadInteger() throws Exception {
    Random rand = new Random(99);
    final int numElements = 256;
    int[] values = rand.ints(numElements).toArray();

    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeIntegers(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    for (int expected : values) {
      assertEquals(expected, reader.readInteger());
    }

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // decodeData transpose specializations: element sizes 2, 12, 16
  // (sizes 4 and 8 are already covered by int/float and long/double tests)
  // ---------------------------------------------------------------------------

  @Test
  public void testFlbaTransposeSize2() throws Exception {
    flbaRoundTrip(2, 512);
  }

  @Test
  public void testFlbaTransposeSize12() throws Exception {
    flbaRoundTrip(12, 256);
  }

  @Test
  public void testFlbaTransposeSize16() throws Exception {
    flbaRoundTrip(16, 256);
  }

  /** Generic FLBA round-trip used to exercise specific element-size transpose paths. */
  private void flbaRoundTrip(int typeLength, int numElements) throws Exception {
    Random rand = new Random(42);
    Binary[] values = new Binary[numElements];
    for (int i = 0; i < numElements; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      values[i] = Binary.fromConstantByteArray(bytes);
    }

    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength,
            numElements * typeLength,
            numElements * typeLength,
            new DirectByteBufferAllocator());
    // Use scalar writes to test the writer's scatter path independently from batch writes.
    for (Binary v : values) {
      writer.writeBytes(v);
    }
    BytesInput input = writer.getBytes();
    assertEquals(numElements * typeLength, input.size());

    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(typeLength);
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    // Scalar read to verify each value
    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readBytes());
    }

    writer.reset();
    writer.close();
  }

  /** Also test the generic fallback path with an odd element size (e.g. 5, 7). */
  @Test
  public void testFlbaTransposeGenericFallback() throws Exception {
    flbaRoundTrip(5, 256);
    flbaRoundTrip(7, 256);
  }

  // ---------------------------------------------------------------------------
  // BATCH_SIZE boundary crossing: tests that internal batch flush works correctly
  // when writing exactly BATCH_SIZE, BATCH_SIZE+1, and multi-BATCH_SIZE counts
  // ---------------------------------------------------------------------------

  @Test
  public void testIntegerWriteExactBatchSize() throws Exception {
    intRoundTrip(BATCH_SIZE);
  }

  @Test
  public void testIntegerWriteBatchSizePlusOne() throws Exception {
    intRoundTrip(BATCH_SIZE + 1);
  }

  @Test
  public void testIntegerWriteMultipleBatches() throws Exception {
    intRoundTrip(BATCH_SIZE * 3 + 17);
  }

  @Test
  public void testLongWriteExactBatchSize() throws Exception {
    longRoundTrip(BATCH_SIZE);
  }

  @Test
  public void testLongWriteBatchSizePlusOne() throws Exception {
    longRoundTrip(BATCH_SIZE + 1);
  }

  @Test
  public void testLongWriteMultipleBatches() throws Exception {
    longRoundTrip(BATCH_SIZE * 3 + 17);
  }

  private void intRoundTrip(int numElements) throws Exception {
    Random rand = new Random(42);
    int[] values = rand.ints(numElements).toArray();

    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    for (int v : values) {
      writer.writeInteger(v);
    }
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    int[] dest = new int[numElements];
    reader.readIntegers(dest, 0, numElements);
    assertArrayEquals(values, dest);

    writer.reset();
    writer.close();
  }

  private void longRoundTrip(int numElements) throws Exception {
    Random rand = new Random(42);
    long[] values = rand.longs(numElements).toArray();

    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    for (long v : values) {
      writer.writeLong(v);
    }
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForLong reader = new ByteStreamSplitValuesReaderForLong();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    long[] dest = new long[numElements];
    reader.readLongs(dest, 0, numElements);
    assertArrayEquals(values, dest);

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // Batch write crossing BATCH_SIZE boundaries via bulk API
  // ---------------------------------------------------------------------------

  @Test
  public void testBulkIntegerWriteCrossesBatchBoundary() throws Exception {
    Random rand = new Random(42);
    final int numElements = BATCH_SIZE * 2 + 13;
    int[] values = rand.ints(numElements).toArray();

    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeIntegers(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    int[] dest = new int[numElements];
    reader.readIntegers(dest, 0, numElements);
    assertArrayEquals(values, dest);

    writer.reset();
    writer.close();
  }

  @Test
  public void testBulkFloatWriteCrossesBatchBoundary() throws Exception {
    Random rand = new Random(42);
    final int numElements = BATCH_SIZE * 2 + 13;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; i++) {
      values[i] = rand.nextFloat();
    }

    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeFloats(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    float[] dest = new float[numElements];
    reader.readFloats(dest, 0, numElements);
    assertArrayEquals(values, dest, 0.0f);

    writer.reset();
    writer.close();
  }

  @Test
  public void testBulkDoubleWriteCrossesBatchBoundary() throws Exception {
    Random rand = new Random(42);
    final int numElements = BATCH_SIZE * 2 + 13;
    double[] values = rand.doubles(numElements).toArray();

    ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    writer.writeDoubles(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForDouble reader = new ByteStreamSplitValuesReaderForDouble();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    double[] dest = new double[numElements];
    reader.readDoubles(dest, 0, numElements);
    assertArrayEquals(values, dest, 0.0);

    writer.reset();
    writer.close();
  }

  @Test
  public void testBulkLongWriteCrossesBatchBoundary() throws Exception {
    Random rand = new Random(42);
    final int numElements = BATCH_SIZE * 2 + 13;
    long[] values = rand.longs(numElements).toArray();

    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    writer.writeLongs(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForLong reader = new ByteStreamSplitValuesReaderForLong();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    long[] dest = new long[numElements];
    reader.readLongs(dest, 0, numElements);
    assertArrayEquals(values, dest);

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // Mixed scalar + batch reads
  // ---------------------------------------------------------------------------

  @Test
  public void testMixedScalarAndBatchReadFloat() throws Exception {
    Random rand = new Random(42);
    final int numElements = 200;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; i++) {
      values[i] = rand.nextFloat();
    }

    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeFloats(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    // Read first 10 scalars
    for (int i = 0; i < 10; i++) {
      assertEquals(values[i], reader.readFloat(), 0.0f);
    }
    // Batch read next 100
    float[] batchDest = new float[100];
    reader.readFloats(batchDest, 0, 100);
    for (int i = 0; i < 100; i++) {
      assertEquals(values[10 + i], batchDest[i], 0.0f);
    }
    // Read remaining 90 scalars
    for (int i = 110; i < numElements; i++) {
      assertEquals(values[i], reader.readFloat(), 0.0f);
    }

    writer.reset();
    writer.close();
  }

  @Test
  public void testMixedScalarAndBatchReadInteger() throws Exception {
    Random rand = new Random(42);
    final int numElements = 200;
    int[] values = rand.ints(numElements).toArray();

    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeIntegers(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    // Scalar, then batch, then scalar
    for (int i = 0; i < 5; i++) {
      assertEquals(values[i], reader.readInteger());
    }
    int[] batchDest = new int[150];
    reader.readIntegers(batchDest, 0, 150);
    for (int i = 0; i < 150; i++) {
      assertEquals(values[5 + i], batchDest[i]);
    }
    for (int i = 155; i < numElements; i++) {
      assertEquals(values[i], reader.readInteger());
    }

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // Edge cases: single value, partial batches with offset
  // ---------------------------------------------------------------------------

  @Test
  public void testSingleValueBatchFloat() throws Exception {
    float value = 3.14f;

    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(4, 4, new DirectByteBufferAllocator());
    writer.writeFloats(new float[] {value}, 0, 1);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(1, ByteBufferInputStream.wrap(input.toByteBuffer()));

    float[] dest = new float[1];
    reader.readFloats(dest, 0, 1);
    assertEquals(value, dest[0], 0.0f);

    writer.reset();
    writer.close();
  }

  @Test
  public void testBatchWriteWithOffset() throws Exception {
    int[] fullArray = {100, 200, 300, 400, 500};

    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            20, 20, new DirectByteBufferAllocator());
    // Write only elements [1..3] (200, 300, 400)
    writer.writeIntegers(fullArray, 1, 3);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(3, ByteBufferInputStream.wrap(input.toByteBuffer()));

    int[] dest = new int[5];
    // Read into dest starting at offset 2
    reader.readIntegers(dest, 2, 3);
    assertEquals(200, dest[2]);
    assertEquals(300, dest[3]);
    assertEquals(400, dest[4]);

    writer.reset();
    writer.close();
  }

  @Test
  public void testBatchReadWithOffset() throws Exception {
    double[] values = {1.1, 2.2, 3.3};

    ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(
            24, 24, new DirectByteBufferAllocator());
    writer.writeDoubles(values, 0, 3);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForDouble reader = new ByteStreamSplitValuesReaderForDouble();
    reader.initFromPage(3, ByteBufferInputStream.wrap(input.toByteBuffer()));

    double[] dest = new double[5];
    reader.readDoubles(dest, 1, 3);
    assertEquals(1.1, dest[1], 0.0);
    assertEquals(2.2, dest[2], 0.0);
    assertEquals(3.3, dest[3], 0.0);

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // advanceByteOffset: overflow should throw
  // ---------------------------------------------------------------------------

  @Test
  public void testBatchReadOverflowThrows() throws Exception {
    final int numElements = 10;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; i++) {
      values[i] = (float) i;
    }

    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeFloats(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    // Read all values
    float[] dest = new float[numElements];
    reader.readFloats(dest, 0, numElements);

    // Attempting to read one more batch should throw
    assertThrows(ParquetDecodingException.class, () -> reader.readFloats(new float[1], 0, 1));

    writer.reset();
    writer.close();
  }

  @Test
  public void testBatchReadPartialOverflowThrows() throws Exception {
    final int numElements = 10;
    int[] values = new int[numElements];
    for (int i = 0; i < numElements; i++) {
      values[i] = i;
    }

    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeIntegers(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    // Read 8 of 10
    int[] dest = new int[8];
    reader.readIntegers(dest, 0, 8);

    // Requesting 3 more (only 2 remain) should throw
    assertThrows(ParquetDecodingException.class, () -> reader.readIntegers(new int[3], 0, 3));

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // getBufferedSize accounts for unflushed batch
  // ---------------------------------------------------------------------------

  @Test
  public void testGetBufferedSizeWithPartialBatch() throws Exception {
    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            256, 256, new DirectByteBufferAllocator());

    // Write fewer than BATCH_SIZE values -- they sit in the internal batch buffer
    for (int i = 0; i < 10; i++) {
      writer.writeInteger(i);
    }
    assertEquals(10 * 4, writer.getBufferedSize());

    // Write more to cross a batch boundary
    for (int i = 0; i < BATCH_SIZE; i++) {
      writer.writeInteger(i);
    }
    assertEquals((10 + BATCH_SIZE) * 4, writer.getBufferedSize());

    writer.reset();
    assertEquals(0, writer.getBufferedSize());
    writer.close();
  }

  @Test
  public void testGetBufferedSizeWithPartialLongBatch() throws Exception {
    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            256, 256, new DirectByteBufferAllocator());

    for (int i = 0; i < 10; i++) {
      writer.writeLong(i);
    }
    assertEquals(10 * 8, writer.getBufferedSize());

    writer.reset();
    assertEquals(0, writer.getBufferedSize());
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // FLBA batch write + batch read for sizes that exercise each transpose path
  // ---------------------------------------------------------------------------

  @Test
  public void testFlbaBatchWriteAndBatchReadSize2() throws Exception {
    flbaBatchRoundTrip(2, 300);
  }

  @Test
  public void testFlbaBatchWriteAndBatchReadSize4() throws Exception {
    flbaBatchRoundTrip(4, 300);
  }

  @Test
  public void testFlbaBatchWriteAndBatchReadSize8() throws Exception {
    flbaBatchRoundTrip(8, 300);
  }

  @Test
  public void testFlbaBatchWriteAndBatchReadSize12() throws Exception {
    flbaBatchRoundTrip(12, 300);
  }

  @Test
  public void testFlbaBatchWriteAndBatchReadSize16() throws Exception {
    flbaBatchRoundTrip(16, 300);
  }

  @Test
  public void testFlbaBatchWriteAndBatchReadSize5() throws Exception {
    flbaBatchRoundTrip(5, 300);
  }

  private void flbaBatchRoundTrip(int typeLength, int numElements) throws Exception {
    Random rand = new Random(42);
    Binary[] values = new Binary[numElements];
    for (int i = 0; i < numElements; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      values[i] = Binary.fromConstantByteArray(bytes);
    }

    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength,
            numElements * typeLength,
            numElements * typeLength,
            new DirectByteBufferAllocator());
    writer.writeBinaries(values, 0, numElements);
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(typeLength);
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    Binary[] dest = new Binary[numElements];
    reader.readBinaries(dest, 0, numElements);
    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], dest[i]);
    }

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // decodeData direct ByteBuffer path (no backing array)
  // ---------------------------------------------------------------------------

  @Test
  public void testDecodeFromDirectByteBuffer() throws Exception {
    Random rand = new Random(42);
    final int numElements = 256;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; i++) {
      values[i] = rand.nextFloat();
    }

    // Encode using standard writer
    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    writer.writeFloats(values, 0, numElements);
    byte[] encoded = writer.getBytes().toByteArray();

    // Copy into a direct ByteBuffer (no backing array) to exercise the else branch in decodeData
    ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length);
    direct.put(encoded);
    direct.flip();

    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(direct));

    float[] dest = new float[numElements];
    reader.readFloats(dest, 0, numElements);
    assertArrayEquals(values, dest, 0.0f);

    writer.reset();
    writer.close();
  }

  @Test
  public void testDecodeFromDirectByteBufferLong() throws Exception {
    Random rand = new Random(42);
    final int numElements = 256;
    long[] values = rand.longs(numElements).toArray();

    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    writer.writeLongs(values, 0, numElements);
    byte[] encoded = writer.getBytes().toByteArray();

    ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length);
    direct.put(encoded);
    direct.flip();

    ByteStreamSplitValuesReaderForLong reader = new ByteStreamSplitValuesReaderForLong();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(direct));

    long[] dest = new long[numElements];
    reader.readLongs(dest, 0, numElements);
    assertArrayEquals(values, dest);

    writer.reset();
    writer.close();
  }

  @Test
  public void testDecodeFromDirectByteBufferFlba() throws Exception {
    Random rand = new Random(42);
    final int numElements = 256;
    final int typeLength = 12;
    Binary[] values = new Binary[numElements];
    for (int i = 0; i < numElements; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      values[i] = Binary.fromConstantByteArray(bytes);
    }

    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength,
            numElements * typeLength,
            numElements * typeLength,
            new DirectByteBufferAllocator());
    writer.writeBinaries(values, 0, numElements);
    byte[] encoded = writer.getBytes().toByteArray();

    ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length);
    direct.put(encoded);
    direct.flip();

    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(typeLength);
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(direct));

    Binary[] dest = new Binary[numElements];
    reader.readBinaries(dest, 0, numElements);
    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], dest[i]);
    }

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // FLBA getBufferedSize with partial batch
  // ---------------------------------------------------------------------------

  @Test
  public void testFlbaGetBufferedSizeWithPartialBatch() throws Exception {
    final int typeLength = 12;
    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength, 256, 256, new DirectByteBufferAllocator());

    Random rand = new Random(42);
    // Write fewer than BATCH_SIZE values -- they sit in batchBufs
    for (int i = 0; i < 10; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      writer.writeBytes(Binary.fromConstantByteArray(bytes));
    }
    assertEquals(10 * typeLength, writer.getBufferedSize());

    // Write more to cross a batch boundary
    for (int i = 0; i < BATCH_SIZE; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      writer.writeBytes(Binary.fromConstantByteArray(bytes));
    }
    assertEquals((10 + BATCH_SIZE) * typeLength, writer.getBufferedSize());

    writer.reset();
    assertEquals(0, writer.getBufferedSize());
    writer.close();
  }
}
