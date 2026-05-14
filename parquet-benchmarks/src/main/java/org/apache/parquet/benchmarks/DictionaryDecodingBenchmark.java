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
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
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
 * Decoding-level micro-benchmarks for the DICTIONARY encoding across
 * {@code LONG}, {@code FLOAT}, {@code DOUBLE}, and {@code FIXED_LEN_BYTE_ARRAY}
 * types — complementing the type-specific dictionary coverage already in
 * {@link IntEncodingBenchmark}, {@link BinaryEncodingBenchmark}, and
 * {@link FixedLenByteArrayEncodingBenchmark}. Encoding benchmarks live in
 * {@link DictionaryEncodingBenchmark}.
 *
 * <p>Decode benchmarks measure the {@link DictionaryValuesReader} lookup path,
 * both per-value and batch. Each invocation decodes {@value #VALUE_COUNT} values;
 * throughput is reported per-value via {@link OperationsPerInvocation}.
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
public class DictionaryDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int MAX_DICT_BYTE_SIZE = 4 * 1024 * 1024;

  // Fixed length for FLBA tests (16 = UUID-sized)
  private static final int FLBA_LENGTH = 16;

  @Param({"LOW_CARDINALITY", "HIGH_CARDINALITY"})
  public String dataPattern;

  // ---- Pre-encoded dictionary pages ----
  private byte[] longDictDataEncoded;
  private Dictionary longDictionary;
  private boolean longDictAvailable;

  private byte[] floatDictDataEncoded;
  private Dictionary floatDictionary;
  private boolean floatDictAvailable;

  private byte[] doubleDictDataEncoded;
  private Dictionary doubleDictionary;
  private boolean doubleDictAvailable;

  private byte[] flbaDictDataEncoded;
  private Dictionary flbaDictionary;
  private boolean flbaDictAvailable;

  // Pre-allocated batch destination arrays (avoid per-invocation allocation artifact)
  private long[] longDest;
  private float[] floatDest;
  private double[] doubleDest;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    int distinct = "LOW_CARDINALITY".equals(dataPattern)
        ? TestDataFactory.LOW_CARDINALITY_DISTINCT
        : 0; // 0 = all unique for HIGH_CARDINALITY

    long seed = TestDataFactory.DEFAULT_SEED;

    long[] longData;
    float[] floatData;
    double[] doubleData;
    Binary[] flbaData;

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

    // Pre-encode for decode benchmarks
    setupLongDict(longData);
    setupFloatDict(floatData);
    setupDoubleDict(doubleData);
    setupFlbaDict(flbaData);

    longDest = new long[VALUE_COUNT];
    floatDest = new float[VALUE_COUNT];
    doubleDest = new double[VALUE_COUNT];
  }

  private void setupLongDict(long[] data) throws IOException {
    DictionaryValuesWriter.PlainLongDictionaryValuesWriter w = new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(
        MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (long v : data) {
      w.writeLong(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    longDictDataEncoded = enc.dictData;
    longDictAvailable = !enc.fellBackToPlain();
    if (longDictAvailable) {
      longDictionary = new PlainValuesDictionary.PlainLongDictionary(enc.dictPage);
    }
  }

  private void setupFloatDict(float[] data) throws IOException {
    DictionaryValuesWriter.PlainFloatDictionaryValuesWriter w = new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(
        MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (float v : data) {
      w.writeFloat(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    floatDictDataEncoded = enc.dictData;
    floatDictAvailable = !enc.fellBackToPlain();
    if (floatDictAvailable) {
      floatDictionary = new PlainValuesDictionary.PlainFloatDictionary(enc.dictPage);
    }
  }

  private void setupDoubleDict(double[] data) throws IOException {
    DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter w = new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(
        MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
    for (double v : data) {
      w.writeDouble(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    doubleDictDataEncoded = enc.dictData;
    doubleDictAvailable = !enc.fellBackToPlain();
    if (doubleDictAvailable) {
      doubleDictionary = new PlainValuesDictionary.PlainDoubleDictionary(enc.dictPage);
    }
  }

  private void setupFlbaDict(Binary[] data) throws IOException {
    DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE, FLBA_LENGTH, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN,
            new HeapByteBufferAllocator());
    for (Binary v : data) {
      w.writeBytes(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    flbaDictDataEncoded = enc.dictData;
    flbaDictAvailable = !enc.fellBackToPlain();
    if (flbaDictAvailable) {
      flbaDictionary = new PlainValuesDictionary.PlainBinaryDictionary(enc.dictPage, FLBA_LENGTH);
    }
  }

  // ==== DECODE BENCHMARKS (per-value) ====

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeLong(Blackhole bh) throws IOException {
    if (!longDictAvailable) return;
    DictionaryValuesReader r = new DictionaryValuesReader(longDictionary);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(longDictDataEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readLong());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFloat(Blackhole bh) throws IOException {
    if (!floatDictAvailable) return;
    DictionaryValuesReader r = new DictionaryValuesReader(floatDictionary);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(floatDictDataEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readFloat());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDouble(Blackhole bh) throws IOException {
    if (!doubleDictAvailable) return;
    DictionaryValuesReader r = new DictionaryValuesReader(doubleDictionary);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(doubleDictDataEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readDouble());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFlba(Blackhole bh) throws IOException {
    if (!flbaDictAvailable) return;
    DictionaryValuesReader r = new DictionaryValuesReader(flbaDictionary);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(flbaDictDataEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readBytes());
    }
  }

  // ==== DECODE BENCHMARKS (batch) ====

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeLongBatch(Blackhole bh) throws IOException {
    if (!longDictAvailable) return;
    DictionaryValuesReader r = new DictionaryValuesReader(longDictionary);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(longDictDataEncoded)));
    r.readLongs(longDest, 0, VALUE_COUNT);
    bh.consume(longDest);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFloatBatch(Blackhole bh) throws IOException {
    if (!floatDictAvailable) return;
    DictionaryValuesReader r = new DictionaryValuesReader(floatDictionary);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(floatDictDataEncoded)));
    r.readFloats(floatDest, 0, VALUE_COUNT);
    bh.consume(floatDest);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDoubleBatch(Blackhole bh) throws IOException {
    if (!doubleDictAvailable) return;
    DictionaryValuesReader r = new DictionaryValuesReader(doubleDictionary);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(doubleDictDataEncoded)));
    r.readDoubles(doubleDest, 0, VALUE_COUNT);
    bh.consume(doubleDest);
  }
}
