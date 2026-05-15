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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link FixedLenByteArrayPlainValuesWriter} and
 * {@link FixedLenByteArrayPlainValuesReader} covering scalar and batch
 * round-trips for fixed-length byte arrays.
 */
public class TestFixedLenByteArrayPlainValuesWriterReader {

  private static final int FIXED_LEN = 12;

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  private FixedLenByteArrayPlainValuesWriter newWriter() {
    return new FixedLenByteArrayPlainValuesWriter(FIXED_LEN, 1024, 64 * 1024, allocator);
  }

  private ByteBufferInputStream wrapForReading(FixedLenByteArrayPlainValuesWriter writer) throws IOException {
    byte[] bytes = writer.getBytes().toByteArray();
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));
  }

  private Binary fixedBinary(int seed) {
    byte[] data = new byte[FIXED_LEN];
    for (int i = 0; i < FIXED_LEN; i++) {
      data[i] = (byte) ((seed + i) & 0xFF);
    }
    return Binary.fromConstantByteArray(data);
  }

  // ---- Encoding metadata ----

  @Test
  public void testEncoding() {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      assertEquals(Encoding.PLAIN, writer.getEncoding());
    }
  }

  // ---- Scalar round-trip ----

  @Test
  public void testScalarRoundTrip() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] expected = {fixedBinary(0), fixedBinary(100), fixedBinary(200)};
      for (Binary v : expected) {
        writer.writeBytes(v);
      }

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertArrayEquals("value at index " + i,
            expected[i].getBytes(), reader.readBytes().getBytes());
      }
    }
  }

  // ---- Batch write, scalar read ----

  @Test
  public void testBatchWriteScalarRead() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] values = {fixedBinary(10), fixedBinary(20), fixedBinary(30)};
      writer.writeBinaries(values, 0, values.length);

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(values.length, wrapForReading(writer));

      for (Binary v : values) {
        assertArrayEquals(v.getBytes(), reader.readBytes().getBytes());
      }
    }
  }

  // ---- Scalar write, batch read ----

  @Test
  public void testScalarWriteBatchRead() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] expected = {fixedBinary(1), fixedBinary(2), fixedBinary(3), fixedBinary(4)};
      for (Binary v : expected) {
        writer.writeBytes(v);
      }

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(expected.length, wrapForReading(writer));

      Binary[] actual = new Binary[expected.length];
      reader.readBinaries(actual, 0, expected.length);
      for (int i = 0; i < expected.length; i++) {
        assertArrayEquals("value at index " + i, expected[i].getBytes(), actual[i].getBytes());
      }
    }
  }

  // ---- Batch round-trip ----

  @Test
  public void testBatchRoundTrip() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] expected = {fixedBinary(50), fixedBinary(60), fixedBinary(70), fixedBinary(80), fixedBinary(90)};
      writer.writeBinaries(expected, 0, expected.length);

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(expected.length, wrapForReading(writer));

      Binary[] actual = new Binary[expected.length];
      reader.readBinaries(actual, 0, expected.length);
      for (int i = 0; i < expected.length; i++) {
        assertArrayEquals("value at index " + i, expected[i].getBytes(), actual[i].getBytes());
      }
    }
  }

  // ---- Batch with offset ----

  @Test
  public void testBatchWriteWithOffset() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] source = {fixedBinary(0), fixedBinary(1), fixedBinary(2), fixedBinary(3), fixedBinary(4)};
      writer.writeBinaries(source, 1, 3); // write [1, 2, 3]

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(3, wrapForReading(writer));

      assertArrayEquals(fixedBinary(1).getBytes(), reader.readBytes().getBytes());
      assertArrayEquals(fixedBinary(2).getBytes(), reader.readBytes().getBytes());
      assertArrayEquals(fixedBinary(3).getBytes(), reader.readBytes().getBytes());
    }
  }

  @Test
  public void testBatchReadWithOffset() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] values = {fixedBinary(10), fixedBinary(20), fixedBinary(30)};
      writer.writeBinaries(values, 0, values.length);

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(values.length, wrapForReading(writer));

      Binary[] dest = new Binary[5];
      reader.readBinaries(dest, 1, 3);
      assertArrayEquals(fixedBinary(10).getBytes(), dest[1].getBytes());
      assertArrayEquals(fixedBinary(20).getBytes(), dest[2].getBytes());
      assertArrayEquals(fixedBinary(30).getBytes(), dest[3].getBytes());
    }
  }

  // ---- Skip ----

  @Test
  public void testSkip() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      writer.writeBytes(fixedBinary(1));
      writer.writeBytes(fixedBinary(2));
      writer.writeBytes(fixedBinary(3));
      writer.writeBytes(fixedBinary(4));

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(4, wrapForReading(writer));

      reader.skip(); // skip 1
      assertArrayEquals(fixedBinary(2).getBytes(), reader.readBytes().getBytes());
      reader.skip(1); // skip 3
      assertArrayEquals(fixedBinary(4).getBytes(), reader.readBytes().getBytes());
    }
  }

  // ---- Skip then batch read ----

  @Test
  public void testSkipThenBatchRead() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] values = {fixedBinary(10), fixedBinary(20), fixedBinary(30), fixedBinary(40)};
      writer.writeBinaries(values, 0, values.length);

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(values.length, wrapForReading(writer));

      reader.skip(2); // skip 10, 20
      Binary[] actual = new Binary[2];
      reader.readBinaries(actual, 0, 2);
      assertArrayEquals(fixedBinary(30).getBytes(), actual[0].getBytes());
      assertArrayEquals(fixedBinary(40).getBytes(), actual[1].getBytes());
    }
  }

  // ---- Wrong length rejection ----

  @Test
  public void testRejectWrongLengthScalar() {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary wrongLen = Binary.fromConstantByteArray(new byte[FIXED_LEN + 1]);
      try {
        writer.writeBytes(wrongLen);
        fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  @Test
  public void testRejectWrongLengthBatch() {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] values = {fixedBinary(1), Binary.fromConstantByteArray(new byte[FIXED_LEN - 1])};
      try {
        writer.writeBinaries(values, 0, values.length);
        fail("Should have thrown exception for wrong length");
      } catch (Exception e) {
        // expected — either IllegalArgumentException or ParquetEncodingException wrapping it
      }
    }
  }

  // ---- Large batch (crosses slab boundaries) ----

  @Test
  public void testLargeBatchRoundTrip() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer =
             new FixedLenByteArrayPlainValuesWriter(FIXED_LEN, 64, 64 * 1024, allocator)) {
      int count = 500;
      Binary[] expected = new Binary[count];
      for (int i = 0; i < count; i++) {
        expected[i] = fixedBinary(i);
      }
      writer.writeBinaries(expected, 0, count);

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(count, wrapForReading(writer));

      Binary[] actual = new Binary[count];
      reader.readBinaries(actual, 0, count);
      for (int i = 0; i < count; i++) {
        assertArrayEquals("value at index " + i, expected[i].getBytes(), actual[i].getBytes());
      }
    }
  }

  // ---- Mixed scalar + batch ----

  @Test
  public void testMixedScalarAndBatchWrite() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      writer.writeBytes(fixedBinary(1));
      Binary[] batch = {fixedBinary(2), fixedBinary(3)};
      writer.writeBinaries(batch, 0, batch.length);
      writer.writeBytes(fixedBinary(4));

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(4, wrapForReading(writer));

      Binary[] actual = new Binary[4];
      reader.readBinaries(actual, 0, 4);
      for (int i = 0; i < 4; i++) {
        assertArrayEquals("value at index " + i, fixedBinary(i + 1).getBytes(), actual[i].getBytes());
      }
    }
  }

  // ---- Reset ----

  @Test
  public void testWriterReset() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      writer.writeBytes(fixedBinary(99));
      writer.reset();
      assertEquals(0, writer.getBufferedSize());

      writer.writeBytes(fixedBinary(42));

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(1, wrapForReading(writer));

      assertArrayEquals(fixedBinary(42).getBytes(), reader.readBytes().getBytes());
    }
  }

  // ---- Empty page ----

  @Test
  public void testEmptyPage() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(0, wrapForReading(writer));
      // Should not throw
    }
  }

  // ---- Small fixed length (e.g. INT96 = 12 bytes, UUID = 16 bytes) ----

  @Test
  public void testSmallFixedLength() throws IOException {
    int len = 4;
    try (FixedLenByteArrayPlainValuesWriter writer =
             new FixedLenByteArrayPlainValuesWriter(len, 1024, 64 * 1024, allocator)) {
      Binary[] expected = new Binary[10];
      for (int i = 0; i < 10; i++) {
        byte[] data = new byte[len];
        for (int j = 0; j < len; j++) {
          data[j] = (byte) (i * len + j);
        }
        expected[i] = Binary.fromConstantByteArray(data);
      }
      writer.writeBinaries(expected, 0, expected.length);

      byte[] bytes = writer.getBytes().toByteArray();
      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(len);
      reader.initFromPage(expected.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)));

      Binary[] actual = new Binary[expected.length];
      reader.readBinaries(actual, 0, expected.length);
      for (int i = 0; i < expected.length; i++) {
        assertArrayEquals("value at index " + i, expected[i].getBytes(), actual[i].getBytes());
      }
    }
  }
}
