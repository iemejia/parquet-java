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
package org.apache.parquet.bytes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCapacityByteArrayOutputStream {

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void testWrite() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      final int expectedSize = 54;
      for (int i = 0; i < expectedSize; i++) {
        capacityByteArrayOutputStream.write(i);
        assertEquals(i + 1, capacityByteArrayOutputStream.size());
      }
      validate(capacityByteArrayOutputStream, expectedSize);
    }
  }

  @Test
  public void testWriteArray() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      int v = 23;
      writeArraysOf3(capacityByteArrayOutputStream, v);
      validate(capacityByteArrayOutputStream, v * 3);
    }
  }

  @Test
  public void testWriteArrayExpand() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(2)) {
      assertEquals(0, capacityByteArrayOutputStream.getCapacity());

      byte[] toWrite = {(byte) (1), (byte) (2), (byte) (3), (byte) (4)};
      int toWriteOffset = 0;
      int writeLength = 2;
      // write 2 bytes array
      capacityByteArrayOutputStream.write(toWrite, toWriteOffset, writeLength);
      toWriteOffset += writeLength;
      assertEquals(2, capacityByteArrayOutputStream.size());
      assertEquals(2, capacityByteArrayOutputStream.getCapacity());

      // write 1 byte array, expand capacity to 4
      writeLength = 1;
      capacityByteArrayOutputStream.write(toWrite, toWriteOffset, writeLength);
      toWriteOffset += writeLength;
      assertEquals(3, capacityByteArrayOutputStream.size());
      assertEquals(4, capacityByteArrayOutputStream.getCapacity());

      // write 1 byte array, not expand
      capacityByteArrayOutputStream.write(toWrite, toWriteOffset, writeLength);
      assertEquals(4, capacityByteArrayOutputStream.size());
      assertEquals(4, capacityByteArrayOutputStream.getCapacity());
      final byte[] byteArray =
          BytesInput.from(capacityByteArrayOutputStream).toByteArray();
      assertArrayEquals(toWrite, byteArray);
    }
  }

  @Test
  public void testWriteArrayAndInt() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      for (int i = 0; i < 23; i++) {
        byte[] toWrite = {(byte) (i * 3), (byte) (i * 3 + 1)};
        capacityByteArrayOutputStream.write(toWrite);
        capacityByteArrayOutputStream.write((byte) (i * 3 + 2));
        assertEquals((i + 1) * 3, capacityByteArrayOutputStream.size());
      }
      validate(capacityByteArrayOutputStream, 23 * 3);
    }
  }

  protected CapacityByteArrayOutputStream newCapacityBAOS(int initialSize) {
    return new CapacityByteArrayOutputStream(initialSize, 1000000, allocator);
  }

  @Test
  public void testReset() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      for (int i = 0; i < 54; i++) {
        capacityByteArrayOutputStream.write(i);
        assertEquals(i + 1, capacityByteArrayOutputStream.size());
      }
      capacityByteArrayOutputStream.reset();
      for (int i = 0; i < 54; i++) {
        capacityByteArrayOutputStream.write(54 + i);
        assertEquals(i + 1, capacityByteArrayOutputStream.size());
      }
      final byte[] byteArray =
          BytesInput.from(capacityByteArrayOutputStream).toByteArray();
      assertEquals(54, byteArray.length);
      for (int i = 0; i < 54; i++) {
        assertEquals(i + " in " + Arrays.toString(byteArray), 54 + i, byteArray[i]);
      }
    }
  }

  @Test
  public void testWriteArrayBiggerThanSlab() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      int v = 23;
      writeArraysOf3(capacityByteArrayOutputStream, v);
      int n = v * 3;
      byte[] toWrite = { // bigger than 2 slabs of size of 10
        (byte) n,
        (byte) (n + 1),
        (byte) (n + 2),
        (byte) (n + 3),
        (byte) (n + 4),
        (byte) (n + 5),
        (byte) (n + 6),
        (byte) (n + 7),
        (byte) (n + 8),
        (byte) (n + 9),
        (byte) (n + 10),
        (byte) (n + 11),
        (byte) (n + 12),
        (byte) (n + 13),
        (byte) (n + 14),
        (byte) (n + 15),
        (byte) (n + 16),
        (byte) (n + 17),
        (byte) (n + 18),
        (byte) (n + 19),
        (byte) (n + 20)
      };
      capacityByteArrayOutputStream.write(toWrite);
      n = n + toWrite.length;
      assertEquals(n, capacityByteArrayOutputStream.size());
      validate(capacityByteArrayOutputStream, n);
      capacityByteArrayOutputStream.reset();
      // check it works after reset too
      capacityByteArrayOutputStream.write(toWrite);
      assertEquals(toWrite.length, capacityByteArrayOutputStream.size());
      byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
      assertEquals(toWrite.length, byteArray.length);
      for (int i = 0; i < toWrite.length; i++) {
        assertEquals(toWrite[i], byteArray[i]);
      }
    }
  }

  @Test
  public void testWriteArrayManySlabs() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      int it = 500;
      int v = 23;
      for (int j = 0; j < it; j++) {
        for (int i = 0; i < v; i++) {
          byte[] toWrite = {(byte) (i * 3), (byte) (i * 3 + 1), (byte) (i * 3 + 2)};
          capacityByteArrayOutputStream.write(toWrite);
          assertEquals((i + 1) * 3 + v * 3 * j, capacityByteArrayOutputStream.size());
        }
      }
      byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
      assertEquals(v * 3 * it, byteArray.length);
      for (int i = 0; i < v * 3 * it; i++) {
        assertEquals(i % (v * 3), byteArray[i]);
      }
      // verifying we have not created 500 * 23 / 10 slabs
      assertTrue(
          "slab count: " + capacityByteArrayOutputStream.getSlabCount(),
          capacityByteArrayOutputStream.getSlabCount() <= 20);
      capacityByteArrayOutputStream.reset();
      writeArraysOf3(capacityByteArrayOutputStream, v);
      validate(capacityByteArrayOutputStream, v * 3);
      // verifying we use less slabs now
      assertTrue(
          "slab count: " + capacityByteArrayOutputStream.getSlabCount(),
          capacityByteArrayOutputStream.getSlabCount() <= 2);
    }
  }

  @Test
  public void testReplaceByte() throws Throwable {
    // test replace the first value
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {
      cbaos.write(10);
      assertEquals(0, cbaos.getCurrentIndex());
      cbaos.setByte(0, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertEquals(7, baos.toByteArray()[0]);
    }

    // test replace value in the first slab
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {
      cbaos.write(10);
      cbaos.write(13);
      cbaos.write(15);
      cbaos.write(17);
      assertEquals(3, cbaos.getCurrentIndex());
      cbaos.write(19);
      cbaos.setByte(3, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertArrayEquals(new byte[] {10, 13, 15, 7, 19}, baos.toByteArray());
    }

    // test replace in *not* the first slab
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {

      // advance part way through the 3rd slab
      for (int i = 0; i < 12; i++) {
        cbaos.write(100 + i);
      }
      assertEquals(11, cbaos.getCurrentIndex());

      cbaos.setByte(6, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertArrayEquals(
          new byte[] {100, 101, 102, 103, 104, 105, 7, 107, 108, 109, 110, 111}, baos.toByteArray());
    }

    // test replace last value of a slab
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {

      // advance part way through the 3rd slab
      for (int i = 0; i < 12; i++) {
        cbaos.write(100 + i);
      }
      assertEquals(11, cbaos.getCurrentIndex());

      cbaos.setByte(9, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertArrayEquals(
          new byte[] {100, 101, 102, 103, 104, 105, 106, 107, 108, 7, 110, 111}, baos.toByteArray());
    }

    // test replace last value
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {

      // advance part way through the 3rd slab
      for (int i = 0; i < 12; i++) {
        cbaos.write(100 + i);
      }
      assertEquals(11, cbaos.getCurrentIndex());

      cbaos.setByte(11, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertArrayEquals(
          new byte[] {100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 7}, baos.toByteArray());
    }
  }

  private void writeArraysOf3(CapacityByteArrayOutputStream capacityByteArrayOutputStream, int n) throws IOException {
    for (int i = 0; i < n; i++) {
      byte[] toWrite = {(byte) (i * 3), (byte) (i * 3 + 1), (byte) (i * 3 + 2)};
      capacityByteArrayOutputStream.write(toWrite);
      assertEquals((i + 1) * 3, capacityByteArrayOutputStream.size());
    }
  }

  private void validate(CapacityByteArrayOutputStream capacityByteArrayOutputStream, final int expectedSize)
      throws IOException {
    final byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
    assertEquals(expectedSize, byteArray.length);
    for (int i = 0; i < expectedSize; i++) {
      assertEquals(i, byteArray[i]);
    }
  }

  // ---- Bulk write methods (writeInt, writeLong, writeInts, writeLongs, writeFloats, writeDoubles) ----

  /**
   * Reads a little-endian int from the byte array at position {@code pos}.
   */
  private static int readIntLE(byte[] b, int pos) {
    return (b[pos] & 0xFF)
        | ((b[pos + 1] & 0xFF) << 8)
        | ((b[pos + 2] & 0xFF) << 16)
        | ((b[pos + 3] & 0xFF) << 24);
  }

  /**
   * Reads a little-endian long from the byte array at position {@code pos}.
   */
  private static long readLongLE(byte[] b, int pos) {
    return (b[pos] & 0xFFL)
        | ((b[pos + 1] & 0xFFL) << 8)
        | ((b[pos + 2] & 0xFFL) << 16)
        | ((b[pos + 3] & 0xFFL) << 24)
        | ((b[pos + 4] & 0xFFL) << 32)
        | ((b[pos + 5] & 0xFFL) << 40)
        | ((b[pos + 6] & 0xFFL) << 48)
        | ((b[pos + 7] & 0xFFL) << 56);
  }

  @Test
  public void testWriteInt() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      int[] values = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 42};
      for (int v : values) {
        cbaos.writeInt(v);
      }
      assertEquals(values.length * 4, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i, values[i], readIntLE(bytes, i * 4));
      }
    }
  }

  @Test
  public void testWriteLong() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      long[] values = {0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 123456789L};
      for (long v : values) {
        cbaos.writeLong(v);
      }
      assertEquals(values.length * 8, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i, values[i], readLongLE(bytes, i * 8));
      }
    }
  }

  @Test
  public void testWriteInts() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(16)) {
      int[] values = {10, 20, 30, 40, 50, 60, 70, 80};
      cbaos.writeInts(values, 0, values.length);
      assertEquals(values.length * 4, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i, values[i], readIntLE(bytes, i * 4));
      }
    }
  }

  @Test
  public void testWriteIntsWithOffset() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      int[] values = {-1, -1, 100, 200, 300, -1, -1};
      cbaos.writeInts(values, 2, 3); // write [100, 200, 300]
      assertEquals(3 * 4, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      assertEquals(100, readIntLE(bytes, 0));
      assertEquals(200, readIntLE(bytes, 4));
      assertEquals(300, readIntLE(bytes, 8));
    }
  }

  @Test
  public void testWriteIntsCrossSlabBoundary() throws Throwable {
    // Initial slab is 10 bytes; 3 ints = 12 bytes, forcing a slab split
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      int[] values = {111, 222, 333};
      cbaos.writeInts(values, 0, values.length);
      assertEquals(12, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i, values[i], readIntLE(bytes, i * 4));
      }
    }
  }

  @Test
  public void testWriteLongs() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(32)) {
      long[] values = {100L, 200L, 300L, 400L};
      cbaos.writeLongs(values, 0, values.length);
      assertEquals(values.length * 8, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i, values[i], readLongLE(bytes, i * 8));
      }
    }
  }

  @Test
  public void testWriteLongsWithOffset() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(32)) {
      long[] values = {-1L, 10L, 20L, 30L, -1L};
      cbaos.writeLongs(values, 1, 3); // write [10, 20, 30]
      assertEquals(3 * 8, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      assertEquals(10L, readLongLE(bytes, 0));
      assertEquals(20L, readLongLE(bytes, 8));
      assertEquals(30L, readLongLE(bytes, 16));
    }
  }

  @Test
  public void testWriteLongsCrossSlabBoundary() throws Throwable {
    // Initial slab is 10 bytes; 2 longs = 16 bytes
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      long[] values = {Long.MIN_VALUE, Long.MAX_VALUE};
      cbaos.writeLongs(values, 0, values.length);
      assertEquals(16, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      assertEquals(Long.MIN_VALUE, readLongLE(bytes, 0));
      assertEquals(Long.MAX_VALUE, readLongLE(bytes, 8));
    }
  }

  @Test
  public void testWriteFloats() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(32)) {
      float[] values = {1.0f, 2.5f, -3.5f, 0.0f, Float.NaN, Float.MAX_VALUE};
      cbaos.writeFloats(values, 0, values.length);
      assertEquals(values.length * 4, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i,
            Float.floatToIntBits(values[i]),
            Float.floatToIntBits(bb.getFloat(i * 4)));
      }
    }
  }

  @Test
  public void testWriteFloatsCrossSlabBoundary() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      float[] values = {1.0f, 2.0f, 3.0f, 4.0f};
      cbaos.writeFloats(values, 0, values.length);
      assertEquals(16, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i,
            Float.floatToIntBits(values[i]),
            Float.floatToIntBits(bb.getFloat(i * 4)));
      }
    }
  }

  @Test
  public void testWriteDoubles() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(64)) {
      double[] values = {1.0, 2.5, -3.5, 0.0, Double.NaN, Double.MAX_VALUE};
      cbaos.writeDoubles(values, 0, values.length);
      assertEquals(values.length * 8, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i,
            Double.doubleToLongBits(values[i]),
            Double.doubleToLongBits(bb.getDouble(i * 8)));
      }
    }
  }

  @Test
  public void testWriteDoublesCrossSlabBoundary() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      double[] values = {Math.PI, Math.E, -1.0};
      cbaos.writeDoubles(values, 0, values.length);
      assertEquals(24, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < values.length; i++) {
        assertEquals("value at index " + i,
            Double.doubleToLongBits(values[i]),
            Double.doubleToLongBits(bb.getDouble(i * 8)));
      }
    }
  }

  @Test
  public void testMixedScalarAndBulkWrites() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(16)) {
      // Write: 1 int scalar, 2 ints bulk, 1 long scalar, 2 longs bulk
      cbaos.writeInt(42);
      int[] ints = {100, 200};
      cbaos.writeInts(ints, 0, ints.length);
      cbaos.writeLong(999L);
      long[] longs = {10000L, 20000L};
      cbaos.writeLongs(longs, 0, longs.length);

      int expectedSize = 4 + 8 + 8 + 16; // 36 bytes
      assertEquals(expectedSize, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      assertEquals(42, readIntLE(bytes, 0));
      assertEquals(100, readIntLE(bytes, 4));
      assertEquals(200, readIntLE(bytes, 8));
      assertEquals(999L, readLongLE(bytes, 12));
      assertEquals(10000L, readLongLE(bytes, 20));
      assertEquals(20000L, readLongLE(bytes, 28));
    }
  }

  @Test
  public void testBulkWriteLargeArray() throws Throwable {
    // Write 1000 ints with a small initial slab to exercise multiple slab allocations
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(16)) {
      int count = 1000;
      int[] values = new int[count];
      for (int i = 0; i < count; i++) {
        values[i] = i * 7 - 3000;
      }
      cbaos.writeInts(values, 0, count);
      assertEquals(count * 4, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      for (int i = 0; i < count; i++) {
        assertEquals("value at index " + i, values[i], readIntLE(bytes, i * 4));
      }
    }
  }

  @Test
  public void testBulkWriteAfterReset() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      int[] first = {1, 2, 3};
      cbaos.writeInts(first, 0, first.length);
      assertEquals(12, cbaos.size());

      cbaos.reset();
      assertEquals(0, cbaos.size());

      int[] second = {10, 20};
      cbaos.writeInts(second, 0, second.length);
      assertEquals(8, cbaos.size());

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      assertEquals(10, readIntLE(bytes, 0));
      assertEquals(20, readIntLE(bytes, 4));
    }
  }
}
