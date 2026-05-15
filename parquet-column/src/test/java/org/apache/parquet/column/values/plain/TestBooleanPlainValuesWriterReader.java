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
import org.apache.parquet.column.Encoding;
import org.junit.Test;

/**
 * Tests for {@link BooleanPlainValuesWriter} and {@link BooleanPlainValuesReader}
 * covering scalar and batch round-trips, edge cases with partial bytes, and skip.
 */
public class TestBooleanPlainValuesWriterReader {

  private ByteBufferInputStream wrapForReading(BooleanPlainValuesWriter writer) throws IOException {
    byte[] bytes = writer.getBytes().toByteArray();
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));
  }

  // ---- Encoding metadata ----

  @Test
  public void testEncoding() {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      assertEquals(Encoding.PLAIN, writer.getEncoding());
    }
  }

  // ---- Scalar round-trip ----

  @Test
  public void testScalarRoundTrip() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] expected = {true, false, true, true, false, false, true, false};
      for (boolean v : expected) {
        writer.writeBoolean(v);
      }

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i, expected[i], reader.readBoolean());
      }
    }
  }

  // ---- Exactly one byte (8 booleans) ----

  @Test
  public void testExactlyOneByte() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] expected = {true, true, true, true, true, true, true, true};
      writer.writeBooleans(expected, 0, expected.length);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      boolean[] actual = new boolean[expected.length];
      reader.readBooleans(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  // ---- Partial byte (< 8 booleans) ----

  @Test
  public void testPartialByte() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] expected = {true, false, true};
      for (boolean v : expected) {
        writer.writeBoolean(v);
      }

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i, expected[i], reader.readBoolean());
      }
    }
  }

  // ---- Batch write, scalar read ----

  @Test
  public void testBatchWriteScalarRead() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] values = {true, false, true, false, true, false, true, false, true, false};
      writer.writeBooleans(values, 0, values.length);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i, values[i], reader.readBoolean());
      }
    }
  }

  // ---- Scalar write, batch read ----

  @Test
  public void testScalarWriteBatchRead() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] expected = {false, true, false, true, true, true, false, false, true};
      for (boolean v : expected) {
        writer.writeBoolean(v);
      }

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      boolean[] actual = new boolean[expected.length];
      reader.readBooleans(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  // ---- Batch round-trip ----

  @Test
  public void testBatchRoundTrip() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] expected = {true, false, false, true, true, false, true, false,
          false, false, true, true, true, false, false, true,
          true};  // 17 values: 2 full bytes + 1 partial
      writer.writeBooleans(expected, 0, expected.length);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      boolean[] actual = new boolean[expected.length];
      reader.readBooleans(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  // ---- Batch with offset ----

  @Test
  public void testBatchWriteWithOffset() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] source = {false, false, true, true, false, false, false};
      writer.writeBooleans(source, 2, 3);  // write [true, true, false]

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      assertEquals(true, reader.readBoolean());
      assertEquals(true, reader.readBoolean());
      assertEquals(false, reader.readBoolean());
    }
  }

  @Test
  public void testBatchReadWithOffset() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] values = {true, false, true};
      writer.writeBooleans(values, 0, values.length);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      boolean[] dest = new boolean[5];
      reader.readBooleans(dest, 1, 3);
      assertEquals(false, dest[0]); // untouched
      assertEquals(true, dest[1]);
      assertEquals(false, dest[2]);
      assertEquals(true, dest[3]);
      assertEquals(false, dest[4]); // untouched
    }
  }

  // ---- Skip ----

  @Test
  public void testSkip() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] values = {true, false, true, false, true};
      writer.writeBooleans(values, 0, values.length);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      reader.skip();  // skip true
      assertEquals(false, reader.readBoolean());
      reader.skip(2); // skip true, false
      assertEquals(true, reader.readBoolean());
    }
  }

  // ---- Skip then batch read ----

  @Test
  public void testSkipThenBatchRead() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] values = {false, false, true, true, false};
      writer.writeBooleans(values, 0, values.length);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      reader.skip(2); // skip false, false
      boolean[] actual = new boolean[3];
      reader.readBooleans(actual, 0, 3);
      assertArrayEquals(new boolean[]{true, true, false}, actual);
    }
  }

  // ---- Batch read starting mid-byte (partial byte prefix in readBooleans) ----

  @Test
  public void testBatchReadStartingMidByte() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      // Write 20 values (2.5 bytes)
      boolean[] values = new boolean[20];
      for (int i = 0; i < 20; i++) {
        values[i] = (i % 3 == 0);
      }
      writer.writeBooleans(values, 0, values.length);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      // Read 3 scalar to advance to mid-byte (bit position 3)
      for (int i = 0; i < 3; i++) {
        assertEquals("scalar read " + i, values[i], reader.readBoolean());
      }

      // Batch read the remaining 17, starting mid-byte
      boolean[] actual = new boolean[17];
      reader.readBooleans(actual, 0, 17);
      boolean[] expectedRemaining = new boolean[17];
      System.arraycopy(values, 3, expectedRemaining, 0, 17);
      assertArrayEquals(expectedRemaining, actual);
    }
  }

  // ---- Large batch ----

  @Test
  public void testLargeBatchRoundTrip() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      int count = 1000;
      boolean[] expected = new boolean[count];
      for (int i = 0; i < count; i++) {
        expected[i] = (i % 2 == 0) || (i % 7 == 0);
      }
      writer.writeBooleans(expected, 0, count);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(count, wrapForReading(writer));

      boolean[] actual = new boolean[count];
      reader.readBooleans(actual, 0, count);
      assertArrayEquals(expected, actual);
    }
  }

  // ---- Mixed scalar + batch write ----

  @Test
  public void testMixedScalarAndBatchWrite() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      // Write 3 scalar (partial byte), then batch of 10, then 2 more scalar
      writer.writeBoolean(true);
      writer.writeBoolean(false);
      writer.writeBoolean(true);
      boolean[] batch = {false, true, false, true, false, true, false, true, false, true};
      writer.writeBooleans(batch, 0, batch.length);
      writer.writeBoolean(true);
      writer.writeBoolean(false);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(15, wrapForReading(writer));

      boolean[] actual = new boolean[15];
      reader.readBooleans(actual, 0, 15);

      boolean[] expected = {true, false, true, false, true, false, true, false, true, false, true, false, true, true, false};
      assertArrayEquals(expected, actual);
    }
  }

  // ---- All false / all true ----

  @Test
  public void testAllFalse() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] expected = new boolean[24]; // all false
      writer.writeBooleans(expected, 0, expected.length);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      boolean[] actual = new boolean[expected.length];
      reader.readBooleans(actual, 0, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void testAllTrue() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      int count = 24;
      boolean[] expected = new boolean[count];
      for (int i = 0; i < count; i++) {
        expected[i] = true;
      }
      writer.writeBooleans(expected, 0, count);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(count, wrapForReading(writer));

      boolean[] actual = new boolean[count];
      reader.readBooleans(actual, 0, count);
      assertArrayEquals(expected, actual);
    }
  }

  // ---- Single value ----

  @Test
  public void testSingleValue() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      writer.writeBoolean(true);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(1, wrapForReading(writer));

      assertEquals(true, reader.readBoolean());
    }
  }

  // ---- Reset ----

  @Test
  public void testWriterReset() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      writer.writeBoolean(true);
      writer.writeBoolean(false);
      writer.reset();
      assertEquals(0, writer.getBufferedSize());

      writer.writeBoolean(false);
      writer.writeBoolean(true);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(2, wrapForReading(writer));

      assertEquals(false, reader.readBoolean());
      assertEquals(true, reader.readBoolean());
    }
  }
}
