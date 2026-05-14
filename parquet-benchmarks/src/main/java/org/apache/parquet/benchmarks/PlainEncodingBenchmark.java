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
package org.apache.parquet.benchmarks;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Encoding micro-benchmarks for the PLAIN encoding across all Parquet primitive types:
 * {@code BOOLEAN}, {@code INT32}, {@code INT64}, {@code FLOAT}, {@code DOUBLE},
 * {@code BINARY}, and {@code FIXED_LEN_BYTE_ARRAY}.
 *
 * <p>Compares per-value scalar writes vs bulk batch writes where batch APIs are
 * available. Batch writes use bulk {@code ByteBuffer} view transfers in
 * {@code CapacityByteArrayOutputStream}.
 *
 * <p>BOOLEAN uses {@link BooleanPlainValuesWriter} which delegates to bit-packing.
 * BINARY uses {@link PlainValuesWriter} (length-prefixed bytes).
 * FIXED_LEN_BYTE_ARRAY uses {@link FixedLenByteArrayPlainValuesWriter} with a
 * representative fixed length of 16 (UUID-sized values).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class PlainEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  /** Representative string length for BINARY benchmarks. */
  private static final int BINARY_STRING_LENGTH = 100;

  /** Representative fixed length for FLBA benchmarks (UUID-sized). */
  private static final int FLBA_LENGTH = 16;

  private boolean[] boolData;
  private int[] intData;
  private long[] longData;
  private float[] floatData;
  private double[] doubleData;
  private Binary[] binaryData;
  private Binary[] flbaData;

  @Setup(Level.Trial)
  public void setup() {
    Random r = new Random(42);
    boolData = new boolean[VALUE_COUNT];
    intData = new int[VALUE_COUNT];
    longData = new long[VALUE_COUNT];
    floatData = new float[VALUE_COUNT];
    doubleData = new double[VALUE_COUNT];
    for (int i = 0; i < VALUE_COUNT; i++) {
      boolData[i] = r.nextBoolean();
      intData[i] = r.nextInt();
      longData[i] = r.nextLong();
      floatData[i] = r.nextFloat();
      doubleData[i] = r.nextDouble();
    }
    binaryData = TestDataFactory.generateBinaryData(VALUE_COUNT, BINARY_STRING_LENGTH, 0, TestDataFactory.DEFAULT_SEED);
    flbaData = TestDataFactory.generateFixedLenByteArrays(VALUE_COUNT, FLBA_LENGTH, 0, TestDataFactory.DEFAULT_SEED);
  }

  private static PlainValuesWriter newWriter() {
    return new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static BooleanPlainValuesWriter newBoolWriter() {
    return new BooleanPlainValuesWriter();
  }

  private FixedLenByteArrayPlainValuesWriter newFlbaWriter() {
    return new FixedLenByteArrayPlainValuesWriter(
        FLBA_LENGTH, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  // ---- BOOLEAN ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeBoolean() throws IOException {
    ValuesWriter w = newBoolWriter();
    for (boolean v : boolData) {
      w.writeBoolean(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeBooleanBatch() throws IOException {
    ValuesWriter w = newBoolWriter();
    w.writeBooleans(boolData, 0, boolData.length);
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- INT32 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeInt() throws IOException {
    PlainValuesWriter w = newWriter();
    for (int v : intData) {
      w.writeInteger(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeIntBatch() throws IOException {
    PlainValuesWriter w = newWriter();
    w.writeIntegers(intData, 0, VALUE_COUNT);
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- INT64 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeLong() throws IOException {
    PlainValuesWriter w = newWriter();
    for (long v : longData) {
      w.writeLong(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeLongBatch() throws IOException {
    PlainValuesWriter w = newWriter();
    w.writeLongs(longData, 0, VALUE_COUNT);
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- FLOAT ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFloat() throws IOException {
    PlainValuesWriter w = newWriter();
    for (float v : floatData) {
      w.writeFloat(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFloatBatch() throws IOException {
    PlainValuesWriter w = newWriter();
    w.writeFloats(floatData, 0, VALUE_COUNT);
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- DOUBLE ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDouble() throws IOException {
    PlainValuesWriter w = newWriter();
    for (double v : doubleData) {
      w.writeDouble(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDoubleBatch() throws IOException {
    PlainValuesWriter w = newWriter();
    w.writeDoubles(doubleData, 0, VALUE_COUNT);
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- BINARY ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeBinary() throws IOException {
    PlainValuesWriter w = newWriter();
    for (Binary v : binaryData) {
      w.writeBytes(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- FIXED_LEN_BYTE_ARRAY ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFixedLenByteArray() throws IOException {
    FixedLenByteArrayPlainValuesWriter w = newFlbaWriter();
    for (Binary v : flbaData) {
      w.writeBytes(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFixedLenByteArrayBatch() throws IOException {
    FixedLenByteArrayPlainValuesWriter w = newFlbaWriter();
    w.writeBinaries(flbaData, 0, flbaData.length);
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }
}
