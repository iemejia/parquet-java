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
package org.apache.parquet.column.values.plain;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link PlainValuesWriter} and {@link PlainValuesReader} covering
 * scalar and batch read/write round-trips for int, long, float, and double.
 */
public class TestPlainValuesWriterReader {

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  private PlainValuesWriter newWriter() {
    return new PlainValuesWriter(1024, 64 * 1024, allocator);
  }

  private ByteBufferInputStream wrapForReading(PlainValuesWriter writer) throws IOException {
    byte[] bytes = writer.getBytes().toByteArray();
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));
  }

  // ---- Encoding metadata ----

  @Test
  public void testEncoding() {
    try (PlainValuesWriter writer = newWriter()) {
      assertEquals(Encoding.PLAIN, writer.getEncoding());
    }
  }

  // ---- Integer scalar ----

  @Test
  public void testIntegerScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      int[] expected = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 42, -42};
      for (int v : expected) {
        writer.writeInteger(v);
      }

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i, expected[i], reader.readInteger());
      }
    }
  }

  // ---- Integer batch ----

  @Test
  public void testIntegerBatchWriteScalarRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      int[] values = {10, 20, 30, 40, 50};
      writer.writeIntegers(values, 0, values.length);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      for (int i = 0; i < values.length; i++) {
        assertEquals(values[i], reader.readInteger());
      }
    }
  }

  @Test
  public void testIntegerScalarWriteBatchRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      int[] expected = {100, 200, 300, 400, 500};
      for (int v : expected) {
        writer.writeInteger(v);
      }

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      int[] actual = new int[expected.length];
      reader.readIntegers(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void testIntegerBatchRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      int[] expected = {0, -1, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, 999, -999, 0};
      writer.writeIntegers(expected, 0, expected.length);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      int[] actual = new int[expected.length];
      reader.readIntegers(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void testIntegerBatchWithOffset() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      int[] source = {-1, -1, 10, 20, 30, -1, -1};
      writer.writeIntegers(source, 2, 3); // write [10, 20, 30]

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      int[] dest = new int[7];
      reader.readIntegers(dest, 2, 3);
      assertEquals(10, dest[2]);
      assertEquals(20, dest[3]);
      assertEquals(30, dest[4]);
    }
  }

  // ---- Long scalar ----

  @Test
  public void testLongScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      long[] expected = {0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 123456789L};
      for (long v : expected) {
        writer.writeLong(v);
      }

      PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i, expected[i], reader.readLong());
      }
    }
  }

  // ---- Long batch ----

  @Test
  public void testLongBatchRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      long[] expected = {0L, Long.MIN_VALUE, Long.MAX_VALUE, -42L, 42L, 0L};
      writer.writeLongs(expected, 0, expected.length);

      PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      long[] actual = new long[expected.length];
      reader.readLongs(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void testLongBatchWriteScalarRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      long[] values = {100L, 200L, 300L};
      writer.writeLongs(values, 0, values.length);

      PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      for (long v : values) {
        assertEquals(v, reader.readLong());
      }
    }
  }

  @Test
  public void testLongScalarWriteBatchRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      long[] expected = {111L, 222L, 333L, 444L};
      for (long v : expected) {
        writer.writeLong(v);
      }

      PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      long[] actual = new long[expected.length];
      reader.readLongs(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  // ---- Float scalar ----

  @Test
  public void testFloatScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      float[] expected = {0.0f, 1.5f, -1.5f, Float.MIN_VALUE, Float.MAX_VALUE,
          Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY};
      for (float v : expected) {
        writer.writeFloat(v);
      }

      PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i,
            Float.floatToIntBits(expected[i]),
            Float.floatToIntBits(reader.readFloat()));
      }
    }
  }

  // ---- Float batch ----

  @Test
  public void testFloatBatchRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      float[] expected = {1.0f, 2.0f, 3.0f, -1.0f, 0.0f, Float.NaN};
      writer.writeFloats(expected, 0, expected.length);

      PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      float[] actual = new float[expected.length];
      reader.readFloats(actual, 0, expected.length);
      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i,
            Float.floatToIntBits(expected[i]),
            Float.floatToIntBits(actual[i]));
      }
    }
  }

  @Test
  public void testFloatBatchWriteScalarRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      float[] values = {1.1f, 2.2f, 3.3f};
      writer.writeFloats(values, 0, values.length);

      PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      for (float v : values) {
        assertEquals(Float.floatToIntBits(v), Float.floatToIntBits(reader.readFloat()));
      }
    }
  }

  // ---- Double scalar ----

  @Test
  public void testDoubleScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      double[] expected = {0.0, 1.5, -1.5, Double.MIN_VALUE, Double.MAX_VALUE,
          Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
      for (double v : expected) {
        writer.writeDouble(v);
      }

      PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i,
            Double.doubleToLongBits(expected[i]),
            Double.doubleToLongBits(reader.readDouble()));
      }
    }
  }

  // ---- Double batch ----

  @Test
  public void testDoubleBatchRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      double[] expected = {1.0, 2.0, 3.0, -1.0, 0.0, Double.NaN};
      writer.writeDoubles(expected, 0, expected.length);

      PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      double[] actual = new double[expected.length];
      reader.readDoubles(actual, 0, expected.length);
      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i,
            Double.doubleToLongBits(expected[i]),
            Double.doubleToLongBits(actual[i]));
      }
    }
  }

  @Test
  public void testDoubleScalarWriteBatchRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      double[] expected = {10.0, 20.0, 30.0, 40.0};
      for (double v : expected) {
        writer.writeDouble(v);
      }

      PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      double[] actual = new double[expected.length];
      reader.readDoubles(actual, 0, expected.length);
      for (int i = 0; i < expected.length; i++) {
        assertEquals(Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(actual[i]));
      }
    }
  }

  // ---- Skip ----

  @Test
  public void testIntegerSkipThenRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeInteger(1);
      writer.writeInteger(2);
      writer.writeInteger(3);
      writer.writeInteger(4);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(4, wrapForReading(writer));

      reader.skip(); // skip 1
      assertEquals(2, reader.readInteger());
      reader.skip(1); // skip 3
      assertEquals(4, reader.readInteger());
    }
  }

  @Test
  public void testLongSkip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeLong(100L);
      writer.writeLong(200L);
      writer.writeLong(300L);

      PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      reader.skip(2);
      assertEquals(300L, reader.readLong());
    }
  }

  @Test
  public void testFloatSkip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);
      writer.writeFloat(3.0f);

      PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      reader.skip();
      assertEquals(Float.floatToIntBits(2.0f), Float.floatToIntBits(reader.readFloat()));
    }
  }

  @Test
  public void testDoubleSkip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeDouble(1.0);
      writer.writeDouble(2.0);
      writer.writeDouble(3.0);

      PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      reader.skip();
      assertEquals(Double.doubleToLongBits(2.0), Double.doubleToLongBits(reader.readDouble()));
    }
  }

  // ---- Skip then batch read ----

  @Test
  public void testSkipThenBatchRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      int[] values = {10, 20, 30, 40, 50};
      writer.writeIntegers(values, 0, values.length);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      reader.skip(2); // skip 10, 20
      int[] actual = new int[3];
      reader.readIntegers(actual, 0, 3);
      assertArrayEquals(new int[]{30, 40, 50}, actual);
    }
  }

  // ---- Large batch (crosses slab boundaries in writer) ----

  @Test
  public void testLargeIntegerBatch() throws IOException {
    // Use small initial size to force multiple slabs
    try (PlainValuesWriter writer = new PlainValuesWriter(64, 64 * 1024, allocator)) {
      int count = 1000;
      int[] expected = new int[count];
      for (int i = 0; i < count; i++) {
        expected[i] = i * 7 - 3000;
      }
      writer.writeIntegers(expected, 0, count);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(count, wrapForReading(writer));

      int[] actual = new int[count];
      reader.readIntegers(actual, 0, count);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void testLargeLongBatch() throws IOException {
    try (PlainValuesWriter writer = new PlainValuesWriter(64, 64 * 1024, allocator)) {
      int count = 500;
      long[] expected = new long[count];
      for (int i = 0; i < count; i++) {
        expected[i] = (long) i * 123456789L - 100000000000L;
      }
      writer.writeLongs(expected, 0, count);

      PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
      reader.initFromPage(count, wrapForReading(writer));

      long[] actual = new long[count];
      reader.readLongs(actual, 0, count);
      assertArrayEquals(expected, actual);
    }
  }

  // ---- Mixed scalar + batch ----

  @Test
  public void testMixedScalarAndBatchIntegerWrite() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeInteger(1);
      writer.writeInteger(2);
      int[] batch = {3, 4, 5};
      writer.writeIntegers(batch, 0, batch.length);
      writer.writeInteger(6);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(6, wrapForReading(writer));

      int[] actual = new int[6];
      reader.readIntegers(actual, 0, 6);
      assertArrayEquals(new int[]{1, 2, 3, 4, 5, 6}, actual);
    }
  }

  @Test
  public void testMixedScalarAndBatchIntegerRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      int[] values = {10, 20, 30, 40, 50};
      writer.writeIntegers(values, 0, values.length);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      assertEquals(10, reader.readInteger());
      int[] batch = new int[3];
      reader.readIntegers(batch, 0, 3);
      assertArrayEquals(new int[]{20, 30, 40}, batch);
      assertEquals(50, reader.readInteger());
    }
  }

  // ---- Reset ----

  @Test
  public void testWriterReset() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeInteger(999);
      writer.reset();
      assertEquals(0, writer.getBufferedSize());

      writer.writeInteger(42);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(1, wrapForReading(writer));

      assertEquals(42, reader.readInteger());
    }
  }

  // ---- Empty page ----

  @Test
  public void testEmptyPage() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(0, wrapForReading(writer));
      // Should not throw — no values to read
    }
  }

  // ---- Direct allocator ----

  @Test
  public void testIntegerRoundTripWithDirectAllocator() throws IOException {
    try (TrackingByteBufferAllocator directAllocator =
             TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator());
         PlainValuesWriter writer = new PlainValuesWriter(1024, 64 * 1024, directAllocator)) {
      int[] expected = {1, 2, 3, 4, 5};
      writer.writeIntegers(expected, 0, expected.length);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      int[] actual = new int[expected.length];
      reader.readIntegers(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }
}
