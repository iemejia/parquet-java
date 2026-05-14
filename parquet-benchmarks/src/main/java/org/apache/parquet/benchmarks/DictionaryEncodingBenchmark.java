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
 * Encoding-level micro-benchmarks for the DICTIONARY encoding across
 * {@code LONG}, {@code FLOAT}, {@code DOUBLE}, and {@code FIXED_LEN_BYTE_ARRAY}
 * types — complementing the type-specific dictionary coverage already in
 * {@link IntEncodingBenchmark}, {@link BinaryEncodingBenchmark}, and
 * {@link FixedLenByteArrayEncodingBenchmark}. Decoding benchmarks live in
 * {@link DictionaryDecodingBenchmark}.
 *
 * <p>Each type's encode benchmark measures the full dictionary-build path
 * (type-specific hash map + id append).
 *
 * <p>The {@code dataPattern} parameter controls cardinality to exercise
 * both the dictionary-hits-only path (LOW_CARDINALITY) and the path
 * where every value is unique (HIGH_CARDINALITY, which may trigger
 * dictionary fallback for large value counts).
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

  // Fixed length for FLBA tests (16 = UUID-sized)
  private static final int FLBA_LENGTH = 16;

  @Param({"LOW_CARDINALITY", "HIGH_CARDINALITY"})
  public String dataPattern;

  private long[] longData;
  private float[] floatData;
  private double[] doubleData;
  private Binary[] flbaData;

  @Setup(Level.Trial)
  public void setup() {
    int distinct = "LOW_CARDINALITY".equals(dataPattern)
        ? TestDataFactory.LOW_CARDINALITY_DISTINCT
        : 0; // 0 = all unique for HIGH_CARDINALITY

    long seed = TestDataFactory.DEFAULT_SEED;

    if (distinct > 0) {
      longData = TestDataFactory.generateLowCardinalityLongs(VALUE_COUNT, distinct, seed);
      floatData = TestDataFactory.generateLowCardinalityFloats(VALUE_COUNT, distinct, seed);
      doubleData = TestDataFactory.generateLowCardinalityDoubles(VALUE_COUNT, distinct, seed);
      flbaData = TestDataFactory.generateFixedLenByteArrays(VALUE_COUNT, FLBA_LENGTH, distinct, seed);
    } else {
      longData = TestDataFactory.generateRandomLongs(VALUE_COUNT, seed);
      floatData = TestDataFactory.generateRandomFloats(VALUE_COUNT, seed);
      doubleData = TestDataFactory.generateRandomDoubles(VALUE_COUNT, seed);
      flbaData = TestDataFactory.generateFixedLenByteArrays(VALUE_COUNT, FLBA_LENGTH, 0, seed);
    }
  }

  // ==== ENCODE BENCHMARKS ====

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeLong(Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainLongDictionaryValuesWriter w = new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(
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
    DictionaryValuesWriter.PlainFloatDictionaryValuesWriter w = new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(
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
    DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter w = new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(
        MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (double v : doubleData) {
      w.writeDouble(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeFlba(Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE, FLBA_LENGTH, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN,
            new HeapByteBufferAllocator());
    for (Binary v : flbaData) {
      w.writeBytes(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }
}
