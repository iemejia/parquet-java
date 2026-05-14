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
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
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
 * Encoding and decoding micro-benchmarks for BINARY delta encodings:
 * DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY. Exercises both encodings across
 * different string lengths.
 *
 * <p>PLAIN encoding benchmarks live in {@link PlainEncodingBenchmark} and
 * {@link PlainDecodingBenchmark}. Dictionary benchmarks live in
 * {@link DictionaryEncodingBenchmark} and {@link DictionaryDecodingBenchmark}.
 *
 * <p>Each benchmark invocation processes {@value #VALUE_COUNT} values. Throughput is
 * reported per-value using {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BinaryEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  @Param({"10", "100", "1000"})
  public int stringLength;

  private Binary[] data;
  private byte[] deltaLengthEncoded;
  private byte[] deltaStringsEncoded;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    data = TestDataFactory.generateBinaryData(VALUE_COUNT, stringLength, 0, TestDataFactory.DEFAULT_SEED);

    // Pre-encode data for decode benchmarks
    deltaLengthEncoded = encodeBinaryWith(newDeltaLengthWriter());
    deltaStringsEncoded = encodeBinaryWith(newDeltaStringsWriter());
  }

  private byte[] encodeBinaryWith(ValuesWriter writer) throws IOException {
    for (Binary v : data) {
      writer.writeBytes(v);
    }
    byte[] bytes = writer.getBytes().toByteArray();
    writer.close();
    return bytes;
  }

  // ---- Writer factories ----

  private static DeltaLengthByteArrayValuesWriter newDeltaLengthWriter() {
    return new DeltaLengthByteArrayValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static DeltaByteArrayWriter newDeltaStringsWriter() {
    return new DeltaByteArrayWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  // ---- Encode benchmarks ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDeltaLengthByteArray() throws IOException {
    return encodeBinaryWith(newDeltaLengthWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDeltaByteArray() throws IOException {
    return encodeBinaryWith(newDeltaStringsWriter());
  }

  // ---- Decode benchmarks ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDeltaLengthByteArray(Blackhole bh) throws IOException {
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(deltaLengthEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDeltaByteArray(Blackhole bh) throws IOException {
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(deltaStringsEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }
}
