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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesReader;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Decoding micro-benchmarks for the PLAIN encoding across all Parquet primitive types:
 * {@code BOOLEAN}, {@code INT32}, {@code INT64}, {@code FLOAT}, {@code DOUBLE},
 * {@code BINARY}, and {@code FIXED_LEN_BYTE_ARRAY}.
 *
 * <p>Each invocation decodes {@value #VALUE_COUNT} values. Per-value methods measure
 * scalar read throughput; batch methods measure bulk array-fill throughput using
 * bulk {@code ByteBuffer} view reads where available.
 *
 * <p>BOOLEAN uses {@link BooleanPlainValuesReader} (bit-unpacking).
 * BINARY uses {@link BinaryPlainValuesReader} (length-prefixed bytes).
 * FIXED_LEN_BYTE_ARRAY uses {@link FixedLenByteArrayPlainValuesReader} with a
 * representative fixed length of 16 (UUID-sized values).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class PlainDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  /** Representative string length for BINARY benchmarks. */
  private static final int BINARY_STRING_LENGTH = 100;

  /** Representative fixed length for FLBA benchmarks (UUID-sized). */
  private static final int FLBA_LENGTH = 16;

  // Pre-encoded pages
  private byte[] boolPage;
  private byte[] intPage;
  private byte[] longPage;
  private byte[] floatPage;
  private byte[] doublePage;
  private byte[] binaryPage;
  private byte[] flbaPage;

  // Pre-allocated destination arrays to avoid per-invocation allocation noise
  private boolean[] boolDest;
  private int[] intDest;
  private long[] longDest;
  private float[] floatDest;
  private double[] doubleDest;
  private Binary[] flbaDest;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random r = new Random(42);

    // Pre-allocate destination arrays
    boolDest = new boolean[VALUE_COUNT];
    intDest = new int[VALUE_COUNT];
    longDest = new long[VALUE_COUNT];
    floatDest = new float[VALUE_COUNT];
    doubleDest = new double[VALUE_COUNT];
    flbaDest = new Binary[VALUE_COUNT];

    // Encode BOOLEAN
    {
      ValuesWriter w = new BooleanPlainValuesWriter();
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeBoolean(r.nextBoolean());
      }
      boolPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode INT32
    {
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeInteger(r.nextInt());
      }
      intPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode INT64
    {
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeLong(r.nextLong());
      }
      longPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode FLOAT
    {
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeFloat(r.nextFloat());
      }
      floatPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode DOUBLE
    {
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeDouble(r.nextDouble());
      }
      doublePage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode BINARY
    {
      Binary[] data = TestDataFactory.generateBinaryData(
          VALUE_COUNT, BINARY_STRING_LENGTH, 0, TestDataFactory.DEFAULT_SEED);
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (Binary v : data) {
        w.writeBytes(v);
      }
      binaryPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode FIXED_LEN_BYTE_ARRAY
    {
      Binary[] data = TestDataFactory.generateFixedLenByteArrays(
          VALUE_COUNT, FLBA_LENGTH, 0, TestDataFactory.DEFAULT_SEED);
      FixedLenByteArrayPlainValuesWriter w = new FixedLenByteArrayPlainValuesWriter(
          FLBA_LENGTH, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (Binary v : data) {
        w.writeBytes(v);
      }
      flbaPage = w.getBytes().toByteArray();
      w.close();
    }
  }

  // ---- BOOLEAN ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeBoolean(Blackhole bh) throws IOException {
    ValuesReader reader = new BooleanPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(boolPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBoolean());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeBooleanBatch(Blackhole bh) throws IOException {
    ValuesReader reader = new BooleanPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(boolPage)));
    reader.readBooleans(boolDest, 0, VALUE_COUNT);
    bh.consume(boolDest);
  }

  // ---- INT32 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeInt(Blackhole bh) throws IOException {
    PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(intPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readInteger());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int[] decodeIntBatch() throws IOException {
    PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(intPage)));
    reader.readIntegers(intDest, 0, VALUE_COUNT);
    return intDest;
  }

  // ---- INT64 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeLong(Blackhole bh) throws IOException {
    PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(longPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readLong());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public long[] decodeLongBatch() throws IOException {
    PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(longPage)));
    reader.readLongs(longDest, 0, VALUE_COUNT);
    return longDest;
  }

  // ---- FLOAT ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFloat(Blackhole bh) throws IOException {
    PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(floatPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readFloat());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public float[] decodeFloatBatch() throws IOException {
    PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(floatPage)));
    reader.readFloats(floatDest, 0, VALUE_COUNT);
    return floatDest;
  }

  // ---- DOUBLE ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDouble(Blackhole bh) throws IOException {
    PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(doublePage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readDouble());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public double[] decodeDoubleBatch() throws IOException {
    PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(doublePage)));
    reader.readDoubles(doubleDest, 0, VALUE_COUNT);
    return doubleDest;
  }

  // ---- BINARY ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeBinary(Blackhole bh) throws IOException {
    BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(binaryPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  // ---- FIXED_LEN_BYTE_ARRAY ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFixedLenByteArray(Blackhole bh) throws IOException {
    FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FLBA_LENGTH);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(flbaPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFixedLenByteArrayBatch(Blackhole bh) throws IOException {
    FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FLBA_LENGTH);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(flbaPage)));
    reader.readBinaries(flbaDest, 0, VALUE_COUNT);
    bh.consume(flbaDest);
  }
}
