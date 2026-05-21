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
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.alp.AlpValuesReaderForDouble;
import org.apache.parquet.column.values.alp.AlpValuesReaderForFloat;
import org.apache.parquet.column.values.alp.AlpValuesWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for the ALP (Adaptive Lossless floating-Point) encoding and decoding.
 *
 * <p>Structured as multiple benchmark classes to isolate different concerns:
 * <ul>
 *   <li>{@link AlpEncodingBenchmarks.PageEncode} — full page encoding including writer allocation
 *       (realistic Parquet usage pattern)</li>
 *   <li>{@link AlpEncodingBenchmarks.SteadyStateEncode} — encoding with writer reuse, isolating
 *       pure encoding cost from allocation overhead</li>
 *   <li>{@link AlpEncodingBenchmarks.PresetEncode} — encoding after presets are warmed, measuring
 *       the fast preset-cached path in isolation</li>
 *   <li>{@link AlpEncodingBenchmarks.Decode} — decoding throughput across distributions and
 *       bit-width/exception-density axes</li>
 *   <li>{@link AlpEncodingBenchmarks.SingleVectorEncode} — per-vector encoding latency measured
 *       with AverageTime mode for reliable sub-millisecond measurements</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * mvn -pl parquet-benchmarks package -DskipTests
 * java -jar parquet-benchmarks/target/parquet-benchmarks.jar AlpEncodingBenchmarks
 * </pre>
 */
public class AlpEncodingBenchmarks {

  private static final int INITIAL_CAPACITY = 4 * 1024 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  /**
   * Data distributions for encoding benchmarks.
   *
   * <p>Chosen to span the ALP performance spectrum from best-case to worst-case:
   * <ul>
   *   <li><b>INTEGER</b>: whole numbers — trivial, zero exceptions, narrow bit-width</li>
   *   <li><b>MONETARY</b>: 2 decimal places — ideal ALP target, zero exceptions</li>
   *   <li><b>SENSOR</b>: 4-6 significant digits, varying exponents — forces broader param search</li>
   *   <li><b>RANDOM</b>: uniform random — produces wide bit-width, stresses bit-packing</li>
   *   <li><b>HIGH_EXCEPTION</b>: ~30% values that fail round-trip — stresses exception handling</li>
   * </ul>
   */
  public enum DataDistribution {
    /** Whole numbers stored as float/double — trivial for ALP (near-zero bit-width, no exceptions). */
    INTEGER,
    /** Decimal monetary values: 2 decimal places, narrow range (0.01 - 9999.99). */
    MONETARY,
    /** Scientific/measurement data: 4-6 significant digits with varying exponents (1e-3 to 1e4). */
    SENSOR,
    /** Uniformly random floating-point values — wide bit-width, stresses bit-packing path. */
    RANDOM,
    /** ~30% exception rate: subnormals, extreme ranges, NaN/Inf mixed with normal values. */
    HIGH_EXCEPTION
  }

  // ==================== Shared Data Generation ====================

  static float[] generateFloatData(DataDistribution distribution, int numValues, Random rand) {
    float[] data = new float[numValues];
    switch (distribution) {
      case INTEGER:
        for (int i = 0; i < numValues; i++) {
          data[i] = (float) rand.nextInt(100000);
        }
        break;
      case MONETARY:
        for (int i = 0; i < numValues; i++) {
          data[i] = Math.round(rand.nextFloat() * 999999) / 100.0f;
        }
        break;
      case SENSOR:
        for (int i = 0; i < numValues; i++) {
          double scale = Math.pow(10, rand.nextInt(8) - 3);
          data[i] = Math.round(rand.nextFloat() * 100000) / 10.0f * (float) scale;
        }
        break;
      case RANDOM:
        for (int i = 0; i < numValues; i++) {
          data[i] = rand.nextFloat() * 100000.0f - 50000.0f;
        }
        break;
      case HIGH_EXCEPTION:
        for (int i = 0; i < numValues; i++) {
          int bucket = rand.nextInt(10);
          if (bucket < 3) {
            // ~30% exceptions: subnormals, NaN, Inf, -0.0, extreme values
            int excType = rand.nextInt(5);
            switch (excType) {
              case 0:
                data[i] = Float.NaN;
                break;
              case 1:
                data[i] = Float.intBitsToFloat(rand.nextInt(0x007FFFFF) + 1); // subnormal
                break;
              case 2:
                data[i] = rand.nextBoolean() ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
                break;
              case 3:
                data[i] = -0.0f;
                break;
              case 4:
                data[i] = Float.intBitsToFloat(rand.nextInt()); // random bit pattern
                break;
            }
          } else {
            data[i] = Math.round(rand.nextFloat() * 99999) / 100.0f;
          }
        }
        break;
    }
    return data;
  }

  static double[] generateDoubleData(DataDistribution distribution, int numValues, Random rand) {
    double[] data = new double[numValues];
    switch (distribution) {
      case INTEGER:
        for (int i = 0; i < numValues; i++) {
          data[i] = (double) rand.nextInt(100000);
        }
        break;
      case MONETARY:
        for (int i = 0; i < numValues; i++) {
          data[i] = Math.round(rand.nextDouble() * 999999) / 100.0;
        }
        break;
      case SENSOR:
        for (int i = 0; i < numValues; i++) {
          double scale = Math.pow(10, rand.nextInt(8) - 3);
          data[i] = Math.round(rand.nextDouble() * 1000000) / 100.0 * scale;
        }
        break;
      case RANDOM:
        for (int i = 0; i < numValues; i++) {
          data[i] = rand.nextDouble() * 100000.0 - 50000.0;
        }
        break;
      case HIGH_EXCEPTION:
        for (int i = 0; i < numValues; i++) {
          int bucket = rand.nextInt(10);
          if (bucket < 3) {
            int excType = rand.nextInt(5);
            switch (excType) {
              case 0:
                data[i] = Double.NaN;
                break;
              case 1:
                data[i] = Double.longBitsToDouble(rand.nextLong() & 0x000FFFFFFFFFFFFFL); // subnormal
                break;
              case 2:
                data[i] = rand.nextBoolean() ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
                break;
              case 3:
                data[i] = -0.0;
                break;
              case 4:
                data[i] = Double.longBitsToDouble(rand.nextLong()); // random bit pattern
                break;
            }
          } else {
            data[i] = Math.round(rand.nextDouble() * 99999) / 100.0;
          }
        }
        break;
    }
    return data;
  }

  static ByteBuffer encodeFloats(float[] values) throws IOException {
    AlpValuesWriter.FloatAlpValuesWriter writer =
        new AlpValuesWriter.FloatAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
    try {
      for (float v : values) {
        writer.writeFloat(v);
      }
      return writer.getBytes().toByteBuffer();
    } finally {
      writer.reset();
      writer.close();
    }
  }

  static ByteBuffer encodeDoubles(double[] values) throws IOException {
    AlpValuesWriter.DoubleAlpValuesWriter writer =
        new AlpValuesWriter.DoubleAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
    try {
      for (double v : values) {
        writer.writeDouble(v);
      }
      return writer.getBytes().toByteBuffer();
    } finally {
      writer.reset();
      writer.close();
    }
  }

  // ==================== PageEncode: Full page encoding with writer allocation ====================

  /**
   * Measures full page encoding cost including writer creation and allocation.
   * This is the realistic Parquet usage pattern: one writer per page.
   *
   * <p>Sizes exclude single-vector (1024) which has unreliable throughput measurements
   * at sub-millisecond durations. Use {@link SingleVectorEncode} for that.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class PageEncode {

    @Param({"INTEGER", "MONETARY", "SENSOR", "RANDOM", "HIGH_EXCEPTION"})
    private DataDistribution distribution;

    @Param({"65536", "131072", "1048576"})
    private int numValues;

    private float[] floatData;
    private double[] doubleData;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatData = generateFloatData(distribution, numValues, rand);
      doubleData = generateDoubleData(distribution, numValues, rand);
    }

    @Benchmark
    public BytesInput encodeFloat() throws IOException {
      AlpValuesWriter.FloatAlpValuesWriter writer =
          new AlpValuesWriter.FloatAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
      try {
        for (float v : floatData) {
          writer.writeFloat(v);
        }
        return writer.getBytes();
      } finally {
        writer.reset();
        writer.close();
      }
    }

    @Benchmark
    public BytesInput encodeDouble() throws IOException {
      AlpValuesWriter.DoubleAlpValuesWriter writer =
          new AlpValuesWriter.DoubleAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
      try {
        for (double v : doubleData) {
          writer.writeDouble(v);
        }
        return writer.getBytes();
      } finally {
        writer.reset();
        writer.close();
      }
    }
  }

  // ==================== SteadyStateEncode: Writer reuse via reset() ====================

  /**
   * Measures pure encoding throughput with writer reuse (no allocation overhead).
   * The writer is created once and reset between invocations, isolating the
   * encoding algorithm cost from object/buffer allocation.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class SteadyStateEncode {

    @Param({"INTEGER", "MONETARY", "SENSOR", "RANDOM", "HIGH_EXCEPTION"})
    private DataDistribution distribution;

    @Param({"65536", "131072"})
    private int numValues;

    private float[] floatData;
    private double[] doubleData;
    private AlpValuesWriter.FloatAlpValuesWriter floatWriter;
    private AlpValuesWriter.DoubleAlpValuesWriter doubleWriter;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatData = generateFloatData(distribution, numValues, rand);
      doubleData = generateDoubleData(distribution, numValues, rand);
      floatWriter =
          new AlpValuesWriter.FloatAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
      doubleWriter =
          new AlpValuesWriter.DoubleAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      floatWriter.close();
      doubleWriter.close();
    }

    @Benchmark
    public BytesInput encodeFloat() throws IOException {
      floatWriter.reset();
      for (float v : floatData) {
        floatWriter.writeFloat(v);
      }
      return floatWriter.getBytes();
    }

    @Benchmark
    public BytesInput encodeDouble() throws IOException {
      doubleWriter.reset();
      for (double v : doubleData) {
        doubleWriter.writeDouble(v);
      }
      return doubleWriter.getBytes();
    }
  }

  // ==================== PresetEncode: Preset-cached encoding path ====================

  /**
   * Measures encoding throughput after preset parameters are locked in.
   *
   * <p>Setup writes enough data (128K values) to activate preset caching, then the
   * benchmark measures a subsequent page encoded entirely with the fast preset path.
   * This isolates the steady-state encoding performance after the sampling phase.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class PresetEncode {

    @Param({"INTEGER", "MONETARY", "SENSOR", "RANDOM"})
    private DataDistribution distribution;

    private static final int BENCH_VALUES = 65536;

    private float[] floatData;
    private double[] doubleData;

    // Writers pre-warmed with enough data to build presets
    private AlpValuesWriter.FloatAlpValuesWriter floatWriter;
    private AlpValuesWriter.DoubleAlpValuesWriter doubleWriter;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      Random rand = new Random(42);

      // Generate warmup data (131072 values = 128 vectors, well past preset threshold of 106)
      int warmupValues = 131072;
      float[] warmupFloats = generateFloatData(distribution, warmupValues, rand);
      double[] warmupDoubles = generateDoubleData(distribution, warmupValues, rand);

      // Generate benchmark data (same distribution, different seed portion)
      floatData = generateFloatData(distribution, BENCH_VALUES, rand);
      doubleData = generateDoubleData(distribution, BENCH_VALUES, rand);

      // Pre-warm writers to activate preset caching
      floatWriter =
          new AlpValuesWriter.FloatAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
      for (float v : warmupFloats) {
        floatWriter.writeFloat(v);
      }
      floatWriter.getBytes(); // finalize to lock presets
      floatWriter.reset(); // reset for benchmark use — presets remain cached

      doubleWriter =
          new AlpValuesWriter.DoubleAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
      for (double v : warmupDoubles) {
        doubleWriter.writeDouble(v);
      }
      doubleWriter.getBytes();
      doubleWriter.reset();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      floatWriter.close();
      doubleWriter.close();
    }

    @Benchmark
    public BytesInput encodeFloatPreset() throws IOException {
      floatWriter.reset();
      for (float v : floatData) {
        floatWriter.writeFloat(v);
      }
      return floatWriter.getBytes();
    }

    @Benchmark
    public BytesInput encodeDoublePreset() throws IOException {
      doubleWriter.reset();
      for (double v : doubleData) {
        doubleWriter.writeDouble(v);
      }
      return doubleWriter.getBytes();
    }
  }

  // ==================== Decode: Decoding throughput ====================

  /**
   * Measures decoding throughput across distributions that stress different decode paths:
   * <ul>
   *   <li>INTEGER: narrow bit-width (fast unpack), no exceptions</li>
   *   <li>RANDOM: wide bit-width (more unpack work), no exceptions</li>
   *   <li>HIGH_EXCEPTION: ~30% exception patching overhead</li>
   *   <li>MONETARY: typical real-world case</li>
   * </ul>
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class Decode {

    @Param({"INTEGER", "MONETARY", "RANDOM", "HIGH_EXCEPTION"})
    private DataDistribution distribution;

    @Param({"65536", "1048576"})
    private int numValues;

    private float[] floatData;
    private double[] doubleData;
    private ByteBuffer floatEncodedBuffer;
    private ByteBuffer doubleEncodedBuffer;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      Random rand = new Random(42);
      floatData = generateFloatData(distribution, numValues, rand);
      doubleData = generateDoubleData(distribution, numValues, rand);
      floatEncodedBuffer = encodeFloats(floatData);
      doubleEncodedBuffer = encodeDoubles(doubleData);
    }

    @Benchmark
    public void decodeFloat(Blackhole bh) throws IOException {
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      ByteBuffer buf = floatEncodedBuffer.duplicate();
      reader.initFromPage(numValues, ByteBufferInputStream.wrap(buf));
      for (int i = 0; i < numValues; i++) {
        bh.consume(reader.readFloat());
      }
    }

    @Benchmark
    public void decodeDouble(Blackhole bh) throws IOException {
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      ByteBuffer buf = doubleEncodedBuffer.duplicate();
      reader.initFromPage(numValues, ByteBufferInputStream.wrap(buf));
      for (int i = 0; i < numValues; i++) {
        bh.consume(reader.readDouble());
      }
    }
  }

  // ==================== SingleVectorEncode: Per-vector latency ====================

  /**
   * Measures single-vector (1024 values) encoding latency using AverageTime mode.
   *
   * <p>Throughput mode is unreliable for sub-millisecond operations with only 3 iterations.
   * AverageTime with microsecond output gives meaningful per-vector cost measurements.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 5, time = 1)
  @Measurement(iterations = 5, time = 2)
  @Fork(value = 2)
  public static class SingleVectorEncode {

    @Param({"INTEGER", "MONETARY", "SENSOR", "RANDOM", "HIGH_EXCEPTION"})
    private DataDistribution distribution;

    private static final int VECTOR_SIZE = 1024;

    private float[] floatData;
    private double[] doubleData;
    private AlpValuesWriter.FloatAlpValuesWriter floatWriter;
    private AlpValuesWriter.DoubleAlpValuesWriter doubleWriter;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatData = generateFloatData(distribution, VECTOR_SIZE, rand);
      doubleData = generateDoubleData(distribution, VECTOR_SIZE, rand);
      floatWriter =
          new AlpValuesWriter.FloatAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
      doubleWriter =
          new AlpValuesWriter.DoubleAlpValuesWriter(INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      floatWriter.close();
      doubleWriter.close();
    }

    @Benchmark
    public BytesInput encodeFloatVector() throws IOException {
      floatWriter.reset();
      for (float v : floatData) {
        floatWriter.writeFloat(v);
      }
      return floatWriter.getBytes();
    }

    @Benchmark
    public BytesInput encodeDoubleVector() throws IOException {
      doubleWriter.reset();
      for (double v : doubleData) {
        doubleWriter.writeDouble(v);
      }
      return doubleWriter.getBytes();
    }
  }
}
