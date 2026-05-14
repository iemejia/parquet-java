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
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Encoding-level micro-benchmarks for the DICTIONARY encoding across all Parquet
 * types that support it: {@code INT32}, {@code INT64}, {@code FLOAT},
 * {@code DOUBLE}, {@code BINARY}, and {@code FIXED_LEN_BYTE_ARRAY}.
 * Decoding benchmarks live in {@link DictionaryDecodingBenchmark}.
 *
 * <p>Fixed-width numeric types ({@code INT32}, {@code INT64}, {@code FLOAT},
 * {@code DOUBLE}) are benchmarked by top-level methods controlled by a class-level
 * {@code dataPattern} parameter. Variable-length types ({@code BINARY} and
 * {@code FIXED_LEN_BYTE_ARRAY}) use inner {@link BinaryState} and {@link FlbaState}
 * classes with their own {@code @Param} dimensions to avoid JMH cross-product
 * pollution.
 *
 * <p>Each type's encode benchmark measures the full dictionary-build path
 * (type-specific hash map + id append). Each invocation encodes
 * {@value #VALUE_COUNT} values; throughput is reported per-value via
 * {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class DictionaryEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int MAX_DICT_BYTE_SIZE = 4 * 1024 * 1024;

  @Param({"LOW_CARDINALITY", "HIGH_CARDINALITY"})
  public String dataPattern;

  private int[] intData;
  private long[] longData;
  private float[] floatData;
  private double[] doubleData;

  @Setup(Level.Trial)
  public void setup() {
    int distinct = "LOW_CARDINALITY".equals(dataPattern)
        ? TestDataFactory.LOW_CARDINALITY_DISTINCT
        : 0; // 0 = all unique for HIGH_CARDINALITY

    long seed = TestDataFactory.DEFAULT_SEED;

    if (distinct > 0) {
      intData = TestDataFactory.generateLowCardinalityInts(VALUE_COUNT, distinct, seed);
      longData = TestDataFactory.generateLowCardinalityLongs(VALUE_COUNT, distinct, seed);
      floatData = TestDataFactory.generateLowCardinalityFloats(VALUE_COUNT, distinct, seed);
      doubleData = TestDataFactory.generateLowCardinalityDoubles(VALUE_COUNT, distinct, seed);
    } else {
      intData = TestDataFactory.generateRandomInts(VALUE_COUNT, seed);
      longData = TestDataFactory.generateRandomLongs(VALUE_COUNT, seed);
      floatData = TestDataFactory.generateRandomFloats(VALUE_COUNT, seed);
      doubleData = TestDataFactory.generateRandomDoubles(VALUE_COUNT, seed);
    }
  }

  // ==== Fixed-width numeric encode benchmarks ====

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeInt(Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (int v : intData) {
      w.writeInteger(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeLong(Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainLongDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (long v : longData) {
      w.writeLong(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeFloat(Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainFloatDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (float v : floatData) {
      w.writeFloat(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeDouble(Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (double v : doubleData) {
      w.writeDouble(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }

  // ==== BINARY (parameterised by stringLength and cardinality) ====

  @State(Scope.Thread)
  public static class BinaryState {
    @Param({"10", "100", "1000"})
    public int stringLength;

    @Param({"LOW", "HIGH"})
    public String cardinality;

    Binary[] data;

    @Setup(Level.Trial)
    public void setup() {
      int distinct = "LOW".equals(cardinality) ? TestDataFactory.LOW_CARDINALITY_DISTINCT : 0;
      data = TestDataFactory.generateBinaryData(
          VALUE_COUNT, stringLength, distinct, TestDataFactory.DEFAULT_SEED);
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeBinary(BinaryState state, Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (Binary v : state.data) {
      w.writeBytes(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }

  // ==== FIXED_LEN_BYTE_ARRAY (parameterised by fixedLength and cardinality) ====

  @State(Scope.Thread)
  public static class FlbaState {
    @Param({"2", "12", "16"})
    public int fixedLength;

    @Param({"LOW", "HIGH"})
    public String cardinality;

    Binary[] data;

    @Setup(Level.Trial)
    public void setup() {
      int distinct = "LOW".equals(cardinality) ? TestDataFactory.LOW_CARDINALITY_DISTINCT : 0;
      data = TestDataFactory.generateFixedLenByteArrays(
          VALUE_COUNT, fixedLength, distinct, TestDataFactory.DEFAULT_SEED);
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeFlba(FlbaState state, Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE, state.fixedLength, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN,
            new HeapByteBufferAllocator());
    for (Binary v : state.data) {
      w.writeBytes(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }
}
