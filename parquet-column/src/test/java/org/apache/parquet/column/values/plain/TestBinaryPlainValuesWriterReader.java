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
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.io.api.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link BinaryPlainValuesReader} (variable-length BINARY)
 * covering scalar and batch round-trips via {@link PlainValuesWriter}.
 */
public class TestBinaryPlainValuesWriterReader {

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

  private Binary binaryFromString(String s) {
    return Binary.fromConstantByteArray(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  // ---- Scalar round-trip ----

  @Test
  public void testScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      String[] strings = {"hello", "", "world", "a", "longer string value"};
      for (String s : strings) {
        writer.writeBytes(binaryFromString(s));
      }

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(strings.length, wrapForReading(writer));

      for (String s : strings) {
        assertEquals(s, reader.readBytes().toStringUsingUTF8());
      }
    }
  }

  // ---- Batch write, scalar read ----

  @Test
  public void testBatchWriteScalarRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      Binary[] values = {
          binaryFromString("alpha"),
          binaryFromString("beta"),
          binaryFromString("gamma")
      };
      writer.writeBinaries(values, 0, values.length);

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      assertEquals("alpha", reader.readBytes().toStringUsingUTF8());
      assertEquals("beta", reader.readBytes().toStringUsingUTF8());
      assertEquals("gamma", reader.readBytes().toStringUsingUTF8());
    }
  }

  // ---- Scalar write, batch read ----

  @Test
  public void testScalarWriteBatchRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      String[] strings = {"one", "two", "three"};
      for (String s : strings) {
        writer.writeBytes(binaryFromString(s));
      }

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(strings.length, wrapForReading(writer));

      Binary[] actual = new Binary[strings.length];
      reader.readBinaries(actual, 0, strings.length);
      for (int i = 0; i < strings.length; i++) {
        assertEquals(strings[i], actual[i].toStringUsingUTF8());
      }
    }
  }

  // ---- Batch round-trip ----

  @Test
  public void testBatchRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      Binary[] expected = {
          binaryFromString("foo"),
          binaryFromString("bar"),
          binaryFromString("baz"),
          binaryFromString(""),
          binaryFromString("qux quux")
      };
      writer.writeBinaries(expected, 0, expected.length);

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      Binary[] actual = new Binary[expected.length];
      reader.readBinaries(actual, 0, expected.length);
      for (int i = 0; i < expected.length; i++) {
        assertEquals("value at index " + i,
            expected[i].toStringUsingUTF8(), actual[i].toStringUsingUTF8());
      }
    }
  }

  // ---- Batch with offset ----

  @Test
  public void testBatchWriteWithOffset() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      Binary[] source = {
          binaryFromString("skip1"),
          binaryFromString("skip2"),
          binaryFromString("keep1"),
          binaryFromString("keep2"),
          binaryFromString("skip3")
      };
      writer.writeBinaries(source, 2, 2); // write [keep1, keep2]

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(2, wrapForReading(writer));

      assertEquals("keep1", reader.readBytes().toStringUsingUTF8());
      assertEquals("keep2", reader.readBytes().toStringUsingUTF8());
    }
  }

  @Test
  public void testBatchReadWithOffset() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      Binary[] values = {
          binaryFromString("a"),
          binaryFromString("b"),
          binaryFromString("c")
      };
      writer.writeBinaries(values, 0, values.length);

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      Binary[] dest = new Binary[5];
      reader.readBinaries(dest, 1, 3);
      assertEquals("a", dest[1].toStringUsingUTF8());
      assertEquals("b", dest[2].toStringUsingUTF8());
      assertEquals("c", dest[3].toStringUsingUTF8());
    }
  }

  // ---- Skip ----

  @Test
  public void testSkip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeBytes(binaryFromString("first"));
      writer.writeBytes(binaryFromString("second"));
      writer.writeBytes(binaryFromString("third"));

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      reader.skip(); // skip "first"
      assertEquals("second", reader.readBytes().toStringUsingUTF8());
    }
  }

  // ---- Skip then batch read ----

  @Test
  public void testSkipThenBatchRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      Binary[] values = {
          binaryFromString("a"),
          binaryFromString("b"),
          binaryFromString("c"),
          binaryFromString("d")
      };
      writer.writeBinaries(values, 0, values.length);

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      reader.skip(); // skip "a"
      Binary[] actual = new Binary[3];
      reader.readBinaries(actual, 0, 3);
      assertEquals("b", actual[0].toStringUsingUTF8());
      assertEquals("c", actual[1].toStringUsingUTF8());
      assertEquals("d", actual[2].toStringUsingUTF8());
    }
  }

  // ---- Binary content (non-UTF8) ----

  @Test
  public void testBinaryContent() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      byte[] raw1 = {0, 1, 2, (byte) 0xFF, (byte) 0xFE};
      byte[] raw2 = {(byte) 0x80, 0};
      Binary bin1 = Binary.fromConstantByteArray(raw1);
      Binary bin2 = Binary.fromConstantByteArray(raw2);

      writer.writeBytes(bin1);
      writer.writeBytes(bin2);

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(2, wrapForReading(writer));

      Binary[] actual = new Binary[2];
      reader.readBinaries(actual, 0, 2);

      assertArrayEquals(raw1, actual[0].getBytes());
      assertArrayEquals(raw2, actual[1].getBytes());
    }
  }

  // ---- Empty binary values ----

  @Test
  public void testEmptyBinaryValues() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      Binary[] values = {
          binaryFromString(""),
          binaryFromString(""),
          binaryFromString("")
      };
      writer.writeBinaries(values, 0, values.length);

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      Binary[] actual = new Binary[values.length];
      reader.readBinaries(actual, 0, values.length);
      for (int i = 0; i < values.length; i++) {
        assertEquals(0, actual[i].length());
      }
    }
  }

  // ---- Large batch ----

  @Test
  public void testLargeBatchRoundTrip() throws IOException {
    try (PlainValuesWriter writer = new PlainValuesWriter(64, 64 * 1024, allocator)) {
      int count = 200;
      Binary[] expected = new Binary[count];
      for (int i = 0; i < count; i++) {
        expected[i] = binaryFromString("value_" + i);
      }
      writer.writeBinaries(expected, 0, count);

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(count, wrapForReading(writer));

      Binary[] actual = new Binary[count];
      reader.readBinaries(actual, 0, count);
      for (int i = 0; i < count; i++) {
        assertEquals("value at index " + i,
            expected[i].toStringUsingUTF8(), actual[i].toStringUsingUTF8());
      }
    }
  }

  // ---- Mixed scalar + batch ----

  @Test
  public void testMixedScalarAndBatchWrite() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeBytes(binaryFromString("first"));
      Binary[] batch = {binaryFromString("second"), binaryFromString("third")};
      writer.writeBinaries(batch, 0, batch.length);
      writer.writeBytes(binaryFromString("fourth"));

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(4, wrapForReading(writer));

      Binary[] actual = new Binary[4];
      reader.readBinaries(actual, 0, 4);
      assertEquals("first", actual[0].toStringUsingUTF8());
      assertEquals("second", actual[1].toStringUsingUTF8());
      assertEquals("third", actual[2].toStringUsingUTF8());
      assertEquals("fourth", actual[3].toStringUsingUTF8());
    }
  }

  // ---- Empty page ----

  @Test
  public void testEmptyPage() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(0, wrapForReading(writer));
      // Should not throw
    }
  }
}
