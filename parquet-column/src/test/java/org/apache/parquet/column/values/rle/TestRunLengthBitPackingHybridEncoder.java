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
package org.apache.parquet.column.values.rle;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.junit.Test;

public class TestRunLengthBitPackingHybridEncoder {

  private RunLengthBitPackingHybridEncoder getRunLengthBitPackingHybridEncoder() {
    return getRunLengthBitPackingHybridEncoder(3, 5, 10);
  }

  private RunLengthBitPackingHybridEncoder getRunLengthBitPackingHybridEncoder(
      int bitWidth, int initialCapacity, int pageSize) {
    return new RunLengthBitPackingHybridEncoder(
        bitWidth, initialCapacity, pageSize, new DirectByteBufferAllocator());
  }

  @Test
  public void testRLEOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(4);
    }
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(5);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 4
    assertEquals(4, BytesUtils.readIntLittleEndianOnOneByte(is));

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 5
    assertEquals(5, BytesUtils.readIntLittleEndianOnOneByte(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testRepeatedZeros() throws Exception {
    // previousValue is initialized to 0
    // make sure that repeated 0s at the beginning
    // of the stream don't trip up the repeat count

    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
    for (int i = 0; i < 10; i++) {
      encoder.writeInt(0);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 10 << 1 = 20
    assertEquals(20, BytesUtils.readUnsignedVarInt(is));
    // payload = 4
    assertEquals(0, BytesUtils.readIntLittleEndianOnOneByte(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testBitWidthZero() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(0, 5, 10);
    for (int i = 0; i < 10; i++) {
      encoder.writeInt(0);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 10 << 1 = 20
    assertEquals(20, BytesUtils.readUnsignedVarInt(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testBitPackingOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(i % 3);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((104/8) << 1) | 1 = 27
    assertEquals(27, BytesUtils.readUnsignedVarInt(is));

    List<Integer> values = unpack(3, 104, is);

    for (int i = 0; i < 100; i++) {
      assertEquals(i % 3, (int) values.get(i));
    }

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testBitPackingOverflow() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();

    for (int i = 0; i < 1000; i++) {
      encoder.writeInt(i % 3);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // 504 is the max number of values in a bit packed run
    // that still has a header of 1 byte
    // header = ((504/8) << 1) | 1 = 127
    assertEquals(127, BytesUtils.readUnsignedVarInt(is));
    List<Integer> values = unpack(3, 504, is);

    for (int i = 0; i < 504; i++) {
      assertEquals(i % 3, (int) values.get(i));
    }

    // there should now be 496 values in another bit-packed run
    // header = ((496/8) << 1) | 1 = 125
    assertEquals(125, BytesUtils.readUnsignedVarInt(is));
    values = unpack(3, 496, is);
    for (int i = 0; i < 496; i++) {
      assertEquals((i + 504) % 3, (int) values.get(i));
    }

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testTransitionFromBitPackingToRle() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();

    // 5 obviously bit-packed values
    encoder.writeInt(0);
    encoder.writeInt(1);
    encoder.writeInt(0);
    encoder.writeInt(1);
    encoder.writeInt(0);

    // three repeated values, that ought to be bit-packed as well
    encoder.writeInt(2);
    encoder.writeInt(2);
    encoder.writeInt(2);

    // lots more repeated values, that should be rle-encoded
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(2);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((8/8) << 1) | 1 = 3
    assertEquals(3, BytesUtils.readUnsignedVarInt(is));

    List<Integer> values = unpack(3, 8, is);
    assertEquals(List.of(0, 1, 0, 1, 0, 2, 2, 2), values);

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 2
    assertEquals(2, BytesUtils.readIntLittleEndianOnOneByte(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testPaddingZerosOnUnfinishedBitPackedRuns() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(5, 5, 10);
    for (int i = 0; i < 9; i++) {
      encoder.writeInt(i + 1);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((16/8) << 1) | 1 = 5
    assertEquals(5, BytesUtils.readUnsignedVarInt(is));

    List<Integer> values = unpack(5, 16, is);

    assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0), values);

    assertEquals(-1, is.read());
  }

  @Test
  public void testSwitchingModes() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(9, 100, 1000);

    // rle first
    for (int i = 0; i < 25; i++) {
      encoder.writeInt(17);
    }

    // bit-packing
    for (int i = 0; i < 7; i++) {
      encoder.writeInt(7);
    }

    encoder.writeInt(8);
    encoder.writeInt(9);
    encoder.writeInt(10);

    // bit-packing followed by rle
    for (int i = 0; i < 25; i++) {
      encoder.writeInt(6);
    }

    // followed by a different rle
    for (int i = 0; i < 8; i++) {
      encoder.writeInt(5);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 25 << 1 = 50
    assertEquals(50, BytesUtils.readUnsignedVarInt(is));
    // payload = 17, stored in 2 bytes
    assertEquals(17, BytesUtils.readIntLittleEndianOnTwoBytes(is));

    // header = ((16/8) << 1) | 1 = 5
    assertEquals(5, BytesUtils.readUnsignedVarInt(is));
    List<Integer> values = unpack(9, 16, is);
    int v = 0;
    for (int i = 0; i < 7; i++) {
      assertEquals(7, (int) values.get(v));
      v++;
    }

    assertEquals(8, (int) values.get(v++));
    assertEquals(9, (int) values.get(v++));
    assertEquals(10, (int) values.get(v++));

    for (int i = 0; i < 6; i++) {
      assertEquals(6, (int) values.get(v));
      v++;
    }

    // header = 19 << 1 = 38
    assertEquals(38, BytesUtils.readUnsignedVarInt(is));
    // payload = 6, stored in 2 bytes
    assertEquals(6, BytesUtils.readIntLittleEndianOnTwoBytes(is));

    // header = 8 << 1  = 16
    assertEquals(16, BytesUtils.readUnsignedVarInt(is));
    // payload = 5, stored in 2 bytes
    assertEquals(5, BytesUtils.readIntLittleEndianOnTwoBytes(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testGroupBoundary() throws Exception {
    byte[] bytes = new byte[2];
    // Create an RLE byte stream that has 3 values (1 literal group) with
    // bit width 2.
    bytes[0] = (1 << 1) | 1;
    bytes[1] = (1 << 0) | (2 << 2) | (3 << 4);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(2, buffer);
    assertEquals(decoder.readInt(), 1);
    assertEquals(decoder.readInt(), 2);
    assertEquals(decoder.readInt(), 3);
    assertEquals(buffer.remaining(), 0);
  }

  // ---- writeInts batch tests ----

  /**
   * Verifies that writeInts produces the same encoded output as writing
   * the same values one-by-one via writeInt.
   */
  private void assertBatchEqualsScalar(int bitWidth, int[] values) throws Exception {
    RunLengthBitPackingHybridEncoder scalar = getRunLengthBitPackingHybridEncoder(bitWidth, 100, 64000);
    for (int v : values) {
      scalar.writeInt(v);
    }
    byte[] scalarBytes = scalar.toBytes().toByteArray();

    RunLengthBitPackingHybridEncoder batch = getRunLengthBitPackingHybridEncoder(bitWidth, 100, 64000);
    batch.writeInts(values, 0, values.length);
    byte[] batchBytes = batch.toBytes().toByteArray();

    // Both must decode to the same values
    RunLengthBitPackingHybridDecoder scalarDec =
        new RunLengthBitPackingHybridDecoder(bitWidth, ByteBuffer.wrap(scalarBytes));
    RunLengthBitPackingHybridDecoder batchDec =
        new RunLengthBitPackingHybridDecoder(bitWidth, ByteBuffer.wrap(batchBytes));

    for (int i = 0; i < values.length; i++) {
      assertEquals("mismatch at index " + i, scalarDec.readInt(), batchDec.readInt());
    }
  }

  @Test
  public void testWriteIntsRleOnly() throws Exception {
    // 100 repeated 4s, then 100 repeated 5s
    int[] values = new int[200];
    for (int i = 0; i < 100; i++) values[i] = 4;
    for (int i = 100; i < 200; i++) values[i] = 5;
    assertBatchEqualsScalar(3, values);
  }

  @Test
  public void testWriteIntsBitPackingOnly() throws Exception {
    // Alternating values -- pure bit-packing
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) values[i] = i % 3;
    assertBatchEqualsScalar(3, values);
  }

  @Test
  public void testWriteIntsMixed() throws Exception {
    // Matches testSwitchingModes: RLE, then bit-packed, then RLE, then RLE
    int[] values = new int[65];
    int idx = 0;
    for (int i = 0; i < 25; i++) values[idx++] = 17;
    for (int i = 0; i < 7; i++) values[idx++] = 7;
    values[idx++] = 8;
    values[idx++] = 9;
    values[idx++] = 10;
    for (int i = 0; i < 25; i++) values[idx++] = 6;
    assertBatchEqualsScalar(9, values);
  }

  @Test
  public void testWriteIntsOverflow() throws Exception {
    // > 504 values to trigger bit-pack overflow boundary
    int[] values = new int[1000];
    for (int i = 0; i < 1000; i++) values[i] = i % 3;
    assertBatchEqualsScalar(3, values);
  }

  @Test
  public void testWriteIntsTailValues() throws Exception {
    // 9 values -- exercises the unfinished bit-packed run padding
    int[] values = new int[9];
    for (int i = 0; i < 9; i++) values[i] = i + 1;
    assertBatchEqualsScalar(5, values);
  }

  @Test
  public void testWriteIntsRepeatedZeros() throws Exception {
    int[] values = new int[10]; // all zeros
    assertBatchEqualsScalar(3, values);
  }

  @Test
  public void testWriteIntsWithOffset() throws Exception {
    // Use offset and length to encode a subset
    int[] values = {99, 99, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 99, 99};
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(3, 100, 64000);
    encoder.writeInts(values, 2, 20);
    byte[] encoded = encoder.toBytes().toByteArray();

    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(3, ByteBuffer.wrap(encoded));
    for (int i = 0; i < 10; i++) assertEquals(4, decoder.readInt());
    for (int i = 0; i < 10; i++) assertEquals(5, decoder.readInt());
  }

  @Test
  public void testWriteIntsIntegration() throws Exception {
    // Mirror the integration test: all bit widths from 0 to 32
    for (int bitWidth = 0; bitWidth <= 32; bitWidth++) {
      long modValue = 1L << bitWidth;

      List<Integer> valuesList = new ArrayList<>();
      for (int i = 0; i < 100; i++) valuesList.add((int) (i % modValue));
      for (int i = 0; i < 100; i++) valuesList.add((int) (77 % modValue));
      for (int i = 0; i < 100; i++) valuesList.add((int) (88 % modValue));
      for (int i = 0; i < 1000; i++) {
        valuesList.add((int) (i % modValue));
        valuesList.add((int) (i % modValue));
        valuesList.add((int) (i % modValue));
      }
      for (int i = 0; i < 1000; i++) valuesList.add((int) (17 % modValue));

      int[] vals = valuesList.stream().mapToInt(Integer::intValue).toArray();
      assertBatchEqualsScalar(bitWidth, vals);
    }
  }

  // ---- writeBooleans / readBooleans batch tests ----

  /**
   * Verifies that writeBooleans produces the same encoded output as writing
   * the same values one-by-one via writeInt(v ? 1 : 0), and that
   * readBooleans round-trips correctly.
   */
  private void assertBooleanBatchEqualsScalar(boolean[] values) throws Exception {
    RunLengthBitPackingHybridEncoder scalar = getRunLengthBitPackingHybridEncoder(1, 100, 64000);
    for (boolean v : values) {
      scalar.writeInt(v ? 1 : 0);
    }
    byte[] scalarBytes = scalar.toBytes().toByteArray();

    RunLengthBitPackingHybridEncoder batch = getRunLengthBitPackingHybridEncoder(1, 100, 64000);
    batch.writeBooleans(values, 0, values.length);
    byte[] batchBytes = batch.toBytes().toByteArray();

    // Both must decode to the same values via readInt
    RunLengthBitPackingHybridDecoder scalarDec =
        new RunLengthBitPackingHybridDecoder(1, ByteBuffer.wrap(scalarBytes));
    RunLengthBitPackingHybridDecoder batchDec =
        new RunLengthBitPackingHybridDecoder(1, ByteBuffer.wrap(batchBytes));
    for (int i = 0; i < values.length; i++) {
      assertEquals("scalar mismatch at index " + i, scalarDec.readInt(), batchDec.readInt());
    }

    // Also verify readBooleans round-trip
    RunLengthBitPackingHybridDecoder boolDec =
        new RunLengthBitPackingHybridDecoder(1, ByteBuffer.wrap(batchBytes));
    boolean[] decoded = new boolean[values.length];
    boolDec.readBooleans(decoded, 0, values.length);
    for (int i = 0; i < values.length; i++) {
      assertEquals("boolean mismatch at index " + i, values[i], decoded[i]);
    }
  }

  @Test
  public void testWriteBooleansAllTrue() throws Exception {
    boolean[] values = new boolean[200];
    java.util.Arrays.fill(values, true);
    assertBooleanBatchEqualsScalar(values);
  }

  @Test
  public void testWriteBooleansAllFalse() throws Exception {
    boolean[] values = new boolean[200]; // all false by default
    assertBooleanBatchEqualsScalar(values);
  }

  @Test
  public void testWriteBooleansAlternating() throws Exception {
    boolean[] values = new boolean[200];
    for (int i = 0; i < 200; i++) values[i] = (i % 2) == 0;
    assertBooleanBatchEqualsScalar(values);
  }

  @Test
  public void testWriteBooleansMixed() throws Exception {
    // Long run of false, short alternating, long run of true
    boolean[] values = new boolean[300];
    // 0-99: false (default)
    for (int i = 100; i < 110; i++) values[i] = (i % 2) == 0; // alternating
    for (int i = 110; i < 300; i++) values[i] = true;
    assertBooleanBatchEqualsScalar(values);
  }

  @Test
  public void testWriteBooleansTailValues() throws Exception {
    // 5 values -- exercises the unfinished bit-packed run padding
    boolean[] values = {true, false, true, true, false};
    assertBooleanBatchEqualsScalar(values);
  }

  @Test
  public void testWriteBooleansWithOffset() throws Exception {
    boolean[] values = {true, true, false, false, false, true, true, true, false, false};
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(1, 100, 64000);
    encoder.writeBooleans(values, 2, 6); // false, false, false, true, true, true
    byte[] encoded = encoder.toBytes().toByteArray();

    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(1, ByteBuffer.wrap(encoded));
    boolean[] decoded = new boolean[6];
    decoder.readBooleans(decoded, 0, 6);
    for (int i = 0; i < 6; i++) {
      assertEquals("mismatch at index " + i, values[2 + i], decoded[i]);
    }
  }

  @Test
  public void testReadBooleansFromScalarEncoded() throws Exception {
    // Encode with scalar writeInt, then decode with batch readBooleans
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(1, 100, 64000);
    boolean[] expected = new boolean[100];
    for (int i = 0; i < 100; i++) {
      expected[i] = (i % 3) == 0;
      encoder.writeInt(expected[i] ? 1 : 0);
    }
    byte[] encoded = encoder.toBytes().toByteArray();

    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(1, ByteBuffer.wrap(encoded));
    boolean[] decoded = new boolean[100];
    decoder.readBooleans(decoded, 0, 100);
    for (int i = 0; i < 100; i++) {
      assertEquals("mismatch at index " + i, expected[i], decoded[i]);
    }
  }

  private static List<Integer> unpack(int bitWidth, int numValues, ByteArrayInputStream is) throws Exception {

    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int[] unpacked = new int[8];
    byte[] next8Values = new byte[bitWidth];

    List<Integer> values = new ArrayList<>(numValues);

    while (values.size() < numValues) {
      for (int i = 0; i < bitWidth; i++) {
        next8Values[i] = (byte) is.read();
      }

      packer.unpack8Values(next8Values, 0, unpacked, 0);

      for (int v = 0; v < 8; v++) {
        values.add(unpacked[v]);
      }
    }

    return values;
  }
}
