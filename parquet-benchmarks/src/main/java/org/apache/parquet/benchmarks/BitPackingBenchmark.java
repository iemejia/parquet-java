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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
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
 * Micro-benchmark targeting the bit-packing unpack hot path in isolation.
 *
 * <p>Compares four cells of the unpack API for a given bit width:
 * <ul>
 *   <li>{@code unpack8Values(byte[], ...)}
 *   <li>{@code unpack8Values(ByteBuffer, ...)}
 *   <li>{@code unpack32Values(byte[], ...)}
 *   <li>{@code unpack32Values(ByteBuffer, ...)}
 * </ul>
 *
 * <p>This is the direct evidence used to evaluate whether callers should prefer
 * the {@code byte[]} form over the {@code ByteBuffer} form, and the 32-at-a-time
 * entry point over the 8-at-a-time one.
 *
 * <p>Bit widths cover common dictionary-id / delta-bit-width cases plus byte-aligned
 * values (8, 16, 32). Both INT32 and INT64 packers are measured since the two code
 * paths are independently generated.
 *
 * <p>Each benchmark invocation unpacks {@value #VALUE_COUNT} values so that results
 * are reported per-value via {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BitPackingBenchmark {

  static final int VALUE_COUNT = 100_000;

  /** Bit widths cover: very small (1), typical dict (4, 7, 10), byte-aligned (8, 16, 24, 32). */
  @Param({"1", "4", "7", "8", "10", "16", "24", "32"})
  public int bitWidth;

  // Int-side state
  private BytePacker intPacker;
  private byte[] intPackedBytes;
  private ByteBuffer intPackedHeap;
  private ByteBuffer intPackedDirect;
  private int[] intOut;

  // Long-side state
  private BytePackerForLong longPacker;
  private byte[] longPackedBytes;
  private ByteBuffer longPackedHeap;
  private ByteBuffer longPackedDirect;
  private long[] longOut;

  private int intGroups; // number of 8-value groups
  private int longGroups;

  @Setup(Level.Trial)
  public void setup() {
    // Round value count down to a multiple of 32 so both unpack8 and unpack32 cover the same range.
    int count = (VALUE_COUNT / 32) * 32;
    intGroups = count / 8;
    longGroups = count / 8;

    intPacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    // bit width 0 has a special packer: output is all zeros. Any packed-bytes buffer (even empty)
    // works for unpack, so we just allocate the minimum required.
    int intBytesRequired = Math.max(1, intGroups * bitWidth);
    intPackedBytes = new byte[intBytesRequired];
    new Random(42).nextBytes(intPackedBytes);
    // Wrap into heap buffer
    intPackedHeap = ByteBuffer.wrap(intPackedBytes.clone());
    // Allocate direct buffer and copy
    intPackedDirect = ByteBuffer.allocateDirect(intBytesRequired);
    intPackedDirect.put(intPackedBytes);
    intPackedDirect.clear();
    intOut = new int[count];

    // Long packer only goes up to bitWidth 64, fine for all our @Param values
    longPacker = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
    int longBytesRequired = Math.max(1, longGroups * bitWidth);
    longPackedBytes = new byte[longBytesRequired];
    new Random(43).nextBytes(longPackedBytes);
    longPackedHeap = ByteBuffer.wrap(longPackedBytes.clone());
    longPackedDirect = ByteBuffer.allocateDirect(longBytesRequired);
    longPackedDirect.put(longPackedBytes);
    longPackedDirect.clear();
    longOut = new long[count];
  }

  // ==========================================================================
  // INT32 unpack benchmarks
  // ==========================================================================

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int[] int_unpack8_byteArray() {
    final int groups = intGroups;
    final int bw = bitWidth;
    final BytePacker p = intPacker;
    final byte[] in = intPackedBytes;
    final int[] out = intOut;
    for (int g = 0, byteOff = 0, valOff = 0; g < groups; g++, byteOff += bw, valOff += 8) {
      p.unpack8Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int[] int_unpack8_byteBuffer_heap() {
    final int groups = intGroups;
    final int bw = bitWidth;
    final BytePacker p = intPacker;
    final ByteBuffer in = intPackedHeap;
    final int[] out = intOut;
    for (int g = 0, byteOff = 0, valOff = 0; g < groups; g++, byteOff += bw, valOff += 8) {
      p.unpack8Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int[] int_unpack8_byteBuffer_direct() {
    final int groups = intGroups;
    final int bw = bitWidth;
    final BytePacker p = intPacker;
    final ByteBuffer in = intPackedDirect;
    final int[] out = intOut;
    for (int g = 0, byteOff = 0, valOff = 0; g < groups; g++, byteOff += bw, valOff += 8) {
      p.unpack8Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int[] int_unpack32_byteArray() {
    final int superGroups = intGroups / 4; // 32-value chunks
    final int step = bitWidth * 4;
    final BytePacker p = intPacker;
    final byte[] in = intPackedBytes;
    final int[] out = intOut;
    for (int sg = 0, byteOff = 0, valOff = 0; sg < superGroups; sg++, byteOff += step, valOff += 32) {
      p.unpack32Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int[] int_unpack32_byteBuffer_heap() {
    final int superGroups = intGroups / 4;
    final int step = bitWidth * 4;
    final BytePacker p = intPacker;
    final ByteBuffer in = intPackedHeap;
    final int[] out = intOut;
    for (int sg = 0, byteOff = 0, valOff = 0; sg < superGroups; sg++, byteOff += step, valOff += 32) {
      p.unpack32Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int[] int_unpack32_byteBuffer_direct() {
    final int superGroups = intGroups / 4;
    final int step = bitWidth * 4;
    final BytePacker p = intPacker;
    final ByteBuffer in = intPackedDirect;
    final int[] out = intOut;
    for (int sg = 0, byteOff = 0, valOff = 0; sg < superGroups; sg++, byteOff += step, valOff += 32) {
      p.unpack32Values(in, byteOff, out, valOff);
    }
    return out;
  }

  // ==========================================================================
  // INT64 unpack benchmarks
  // ==========================================================================

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public long[] long_unpack8_byteArray() {
    final int groups = longGroups;
    final int bw = bitWidth;
    final BytePackerForLong p = longPacker;
    final byte[] in = longPackedBytes;
    final long[] out = longOut;
    for (int g = 0, byteOff = 0, valOff = 0; g < groups; g++, byteOff += bw, valOff += 8) {
      p.unpack8Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public long[] long_unpack8_byteBuffer_heap() {
    final int groups = longGroups;
    final int bw = bitWidth;
    final BytePackerForLong p = longPacker;
    final ByteBuffer in = longPackedHeap;
    final long[] out = longOut;
    for (int g = 0, byteOff = 0, valOff = 0; g < groups; g++, byteOff += bw, valOff += 8) {
      p.unpack8Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public long[] long_unpack8_byteBuffer_direct() {
    final int groups = longGroups;
    final int bw = bitWidth;
    final BytePackerForLong p = longPacker;
    final ByteBuffer in = longPackedDirect;
    final long[] out = longOut;
    for (int g = 0, byteOff = 0, valOff = 0; g < groups; g++, byteOff += bw, valOff += 8) {
      p.unpack8Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public long[] long_unpack32_byteArray() {
    final int superGroups = longGroups / 4;
    final int step = bitWidth * 4;
    final BytePackerForLong p = longPacker;
    final byte[] in = longPackedBytes;
    final long[] out = longOut;
    for (int sg = 0, byteOff = 0, valOff = 0; sg < superGroups; sg++, byteOff += step, valOff += 32) {
      p.unpack32Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public long[] long_unpack32_byteBuffer_heap() {
    final int superGroups = longGroups / 4;
    final int step = bitWidth * 4;
    final BytePackerForLong p = longPacker;
    final ByteBuffer in = longPackedHeap;
    final long[] out = longOut;
    for (int sg = 0, byteOff = 0, valOff = 0; sg < superGroups; sg++, byteOff += step, valOff += 32) {
      p.unpack32Values(in, byteOff, out, valOff);
    }
    return out;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public long[] long_unpack32_byteBuffer_direct() {
    final int superGroups = longGroups / 4;
    final int step = bitWidth * 4;
    final BytePackerForLong p = longPacker;
    final ByteBuffer in = longPackedDirect;
    final long[] out = longOut;
    for (int sg = 0, byteOff = 0, valOff = 0; sg < superGroups; sg++, byteOff += step, valOff += 32) {
      p.unpack32Values(in, byteOff, out, valOff);
    }
    return out;
  }

  // Consume the output array in a way that prevents dead-code elimination, for any consumer
  // calling these methods outside JMH. (JMH itself consumes the returned array.)
  @SuppressWarnings("unused")
  private static void consume(Blackhole bh, int[] a) {
    bh.consume(a);
  }
}
