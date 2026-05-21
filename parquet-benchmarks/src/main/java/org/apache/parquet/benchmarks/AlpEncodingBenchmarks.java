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

  // ==================== ExceptionRateGradient: Throughput vs exception rate ====================

  /**
   * Measures encoding throughput as exception rate increases from 1% to 50%.
   *
   * <p>This characterizes the relationship between exception density and encoding cost,
   * revealing the inflection point where ALP becomes inefficient and a fallback (like
   * ALP-RD or BYTE_STREAM_SPLIT) would be preferable.
   *
   * <p>Exception values are NaN/Inf/subnormal injected at the specified rate into
   * otherwise-clean monetary data, ensuring the exception rate is the only variable.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class ExceptionRateGradient {

    @Param({"1", "5", "10", "20", "30", "50"})
    private int exceptionPercent;

    private static final int NUM_VALUES = 65536;

    private float[] floatData;
    private double[] doubleData;
    private AlpValuesWriter.FloatAlpValuesWriter floatWriter;
    private AlpValuesWriter.DoubleAlpValuesWriter doubleWriter;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatData = new float[NUM_VALUES];
      doubleData = new double[NUM_VALUES];

      for (int i = 0; i < NUM_VALUES; i++) {
        if (rand.nextInt(100) < exceptionPercent) {
          // Exception: mix of NaN, Inf, subnormal, -0.0
          int excType = rand.nextInt(4);
          switch (excType) {
            case 0: floatData[i] = Float.NaN; doubleData[i] = Double.NaN; break;
            case 1: floatData[i] = Float.POSITIVE_INFINITY; doubleData[i] = Double.POSITIVE_INFINITY; break;
            case 2: floatData[i] = Float.NEGATIVE_INFINITY; doubleData[i] = Double.NEGATIVE_INFINITY; break;
            case 3: floatData[i] = -0.0f; doubleData[i] = -0.0; break;
          }
        } else {
          // Clean monetary value
          float fv = Math.round(rand.nextFloat() * 999999) / 100.0f;
          floatData[i] = fv;
          doubleData[i] = Math.round(rand.nextDouble() * 999999) / 100.0;
        }
      }

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

    @Benchmark
    public void decodeFloat(Blackhole bh) throws IOException {
      floatWriter.reset();
      for (float v : floatData) floatWriter.writeFloat(v);
      ByteBuffer buf = floatWriter.getBytes().toByteBuffer();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(NUM_VALUES, ByteBufferInputStream.wrap(buf));
      for (int i = 0; i < NUM_VALUES; i++) bh.consume(reader.readFloat());
    }

    @Benchmark
    public void decodeDouble(Blackhole bh) throws IOException {
      doubleWriter.reset();
      for (double v : doubleData) doubleWriter.writeDouble(v);
      ByteBuffer buf = doubleWriter.getBytes().toByteBuffer();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(NUM_VALUES, ByteBufferInputStream.wrap(buf));
      for (int i = 0; i < NUM_VALUES; i++) bh.consume(reader.readDouble());
    }
  }

  // ==================== VectorSizeVariation: Impact of vector size on throughput ====================

  /**
   * Measures encoding throughput at different vector sizes (8, 64, 256, 1024, 4096, 32768).
   *
   * <p>Vector size affects: header overhead per vector (fixed cost amortized over fewer values),
   * L1 cache locality (smaller vectors fit in L1), and brute-force search cost per vector.
   * This validates the default choice of 1024 and reveals tradeoffs.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class VectorSizeVariation {

    @Param({"8", "64", "256", "1024", "4096", "32768"})
    private int vectorSize;

    @Param({"MONETARY", "SENSOR"})
    private DataDistribution distribution;

    private static final int NUM_VALUES = 65536;

    private float[] floatData;
    private double[] doubleData;
    private AlpValuesWriter.FloatAlpValuesWriter floatWriter;
    private AlpValuesWriter.DoubleAlpValuesWriter doubleWriter;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatData = generateFloatData(distribution, NUM_VALUES, rand);
      doubleData = generateDoubleData(distribution, NUM_VALUES, rand);
      floatWriter = new AlpValuesWriter.FloatAlpValuesWriter(
          INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator(), vectorSize);
      doubleWriter = new AlpValuesWriter.DoubleAlpValuesWriter(
          INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator(), vectorSize);
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

  // ==================== MultiPageColumnChunk: Sequential page encoding ====================

  /**
   * Simulates writing a column chunk with multiple pages, measuring throughput
   * across sequential reset-write-getBytes cycles.
   *
   * <p>This exposes the preset-cache-reset penalty: presets are currently lost on reset(),
   * so each page re-performs brute-force parameter search for the first 8 vectors.
   * With 10 pages of 65K values each, this measures real-world column-chunk throughput.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class MultiPageColumnChunk {

    @Param({"MONETARY", "SENSOR"})
    private DataDistribution distribution;

    @Param({"10", "20"})
    private int numPages;

    private static final int VALUES_PER_PAGE = 65536;

    private float[][] floatPages;
    private double[][] doublePages;
    private AlpValuesWriter.FloatAlpValuesWriter floatWriter;
    private AlpValuesWriter.DoubleAlpValuesWriter doubleWriter;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatPages = new float[numPages][];
      doublePages = new double[numPages][];
      for (int p = 0; p < numPages; p++) {
        floatPages[p] = generateFloatData(distribution, VALUES_PER_PAGE, rand);
        doublePages[p] = generateDoubleData(distribution, VALUES_PER_PAGE, rand);
      }
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

    /** Encodes numPages pages sequentially, returning total bytes written. */
    @Benchmark
    public long encodeFloatColumnChunk() throws IOException {
      long totalBytes = 0;
      for (int p = 0; p < numPages; p++) {
        floatWriter.reset();
        for (float v : floatPages[p]) {
          floatWriter.writeFloat(v);
        }
        totalBytes += floatWriter.getBytes().size();
      }
      return totalBytes;
    }

    @Benchmark
    public long encodeDoubleColumnChunk() throws IOException {
      long totalBytes = 0;
      for (int p = 0; p < numPages; p++) {
        doubleWriter.reset();
        for (double v : doublePages[p]) {
          doubleWriter.writeDouble(v);
        }
        totalBytes += doubleWriter.getBytes().size();
      }
      return totalBytes;
    }
  }

  // ==================== MixedDistribution: Distribution shift mid-page ====================

  /**
   * Simulates data with a distribution change mid-page (e.g., sensor data that
   * transitions between normal readings and anomalous values).
   *
   * <p>The first half is clean MONETARY data, the second half is SENSOR data with
   * different scaling. This tests whether the sampler picks parameters robust to
   * distribution shifts, and measures the cost of suboptimal preset reuse.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class MixedDistribution {

    /** How the distributions are mixed within a page. */
    public enum MixPattern {
      /** First half MONETARY, second half SENSOR */
      HALF_HALF,
      /** Alternating blocks of 1024: MONETARY, SENSOR, MONETARY, SENSOR... */
      ALTERNATING_BLOCKS,
      /** 90% MONETARY with 10% random SENSOR bursts scattered throughout */
      BURST_ANOMALY
    }

    @Param({"HALF_HALF", "ALTERNATING_BLOCKS", "BURST_ANOMALY"})
    private MixPattern mixPattern;

    private static final int NUM_VALUES = 65536;

    private float[] floatData;
    private double[] doubleData;
    private AlpValuesWriter.FloatAlpValuesWriter floatWriter;
    private AlpValuesWriter.DoubleAlpValuesWriter doubleWriter;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatData = new float[NUM_VALUES];
      doubleData = new double[NUM_VALUES];

      switch (mixPattern) {
        case HALF_HALF: {
          float[] monetary = generateFloatData(DataDistribution.MONETARY, NUM_VALUES / 2, rand);
          float[] sensor = generateFloatData(DataDistribution.SENSOR, NUM_VALUES / 2, rand);
          System.arraycopy(monetary, 0, floatData, 0, NUM_VALUES / 2);
          System.arraycopy(sensor, 0, floatData, NUM_VALUES / 2, NUM_VALUES / 2);
          double[] monetaryD = generateDoubleData(DataDistribution.MONETARY, NUM_VALUES / 2, rand);
          double[] sensorD = generateDoubleData(DataDistribution.SENSOR, NUM_VALUES / 2, rand);
          System.arraycopy(monetaryD, 0, doubleData, 0, NUM_VALUES / 2);
          System.arraycopy(sensorD, 0, doubleData, NUM_VALUES / 2, NUM_VALUES / 2);
          break;
        }
        case ALTERNATING_BLOCKS: {
          int blockSize = 1024;
          for (int i = 0; i < NUM_VALUES; i += blockSize) {
            DataDistribution dist = ((i / blockSize) % 2 == 0)
                ? DataDistribution.MONETARY : DataDistribution.SENSOR;
            float[] block = generateFloatData(dist, blockSize, rand);
            double[] blockD = generateDoubleData(dist, blockSize, rand);
            System.arraycopy(block, 0, floatData, i, blockSize);
            System.arraycopy(blockD, 0, doubleData, i, blockSize);
          }
          break;
        }
        case BURST_ANOMALY: {
          // 90% monetary base
          float[] base = generateFloatData(DataDistribution.MONETARY, NUM_VALUES, rand);
          double[] baseD = generateDoubleData(DataDistribution.MONETARY, NUM_VALUES, rand);
          System.arraycopy(base, 0, floatData, 0, NUM_VALUES);
          System.arraycopy(baseD, 0, doubleData, 0, NUM_VALUES);
          // Inject 10% sensor bursts at random positions
          float[] bursts = generateFloatData(DataDistribution.SENSOR, NUM_VALUES / 10, rand);
          double[] burstsD = generateDoubleData(DataDistribution.SENSOR, NUM_VALUES / 10, rand);
          for (int i = 0; i < bursts.length; i++) {
            int pos = rand.nextInt(NUM_VALUES);
            floatData[pos] = bursts[i];
            doubleData[pos] = burstsD[i];
          }
          break;
        }
      }

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

  // ==================== CompressionRatio: Space efficiency measurement ====================

  /**
   * Measures compression ratio (encoded bytes / raw bytes) across distributions.
   *
   * <p>Uses SingleShotTime mode with 1 fork to report compressed sizes quickly.
   * The actual compressed size is printed in tearDown for each distribution.
   * The benchmark "score" is the encoding time for reference.
   *
   * <p>Run with: {@code java -jar parquet-benchmarks.jar CompressionRatio -f 1 -wi 0 -i 1}
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  @Fork(value = 1)
  public static class CompressionRatio {

    @Param({"INTEGER", "MONETARY", "SENSOR", "RANDOM", "HIGH_EXCEPTION"})
    private DataDistribution distribution;

    private static final int NUM_VALUES = 65536;

    private float[] floatData;
    private double[] doubleData;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatData = generateFloatData(distribution, NUM_VALUES, rand);
      doubleData = generateDoubleData(distribution, NUM_VALUES, rand);
    }

    @Benchmark
    public void measureFloatCompression(Blackhole bh) throws IOException {
      AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter(
          INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
      try {
        for (float v : floatData) writer.writeFloat(v);
        BytesInput bytes = writer.getBytes();
        long encodedSize = bytes.size();
        long rawSize = (long) NUM_VALUES * 4;
        System.out.printf("  >>> [%s] Float: %d -> %d bytes (%.2fx compression, %.1f%% of raw)%n",
            distribution, rawSize, encodedSize,
            (double) rawSize / encodedSize,
            100.0 * encodedSize / rawSize);
        bh.consume(bytes);
      } finally {
        writer.reset();
        writer.close();
      }
    }

    @Benchmark
    public void measureDoubleCompression(Blackhole bh) throws IOException {
      AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter(
          INITIAL_CAPACITY, PAGE_SIZE, new DirectByteBufferAllocator());
      try {
        for (double v : doubleData) writer.writeDouble(v);
        BytesInput bytes = writer.getBytes();
        long encodedSize = bytes.size();
        long rawSize = (long) NUM_VALUES * 8;
        System.out.printf("  >>> [%s] Double: %d -> %d bytes (%.2fx compression, %.1f%% of raw)%n",
            distribution, rawSize, encodedSize,
            (double) rawSize / encodedSize,
            100.0 * encodedSize / rawSize);
        bh.consume(bytes);
      } finally {
        writer.reset();
        writer.close();
      }
    }
  }

  // ==================== PartialVector: Non-aligned value counts ====================

  /**
   * Measures encoding throughput for non-power-of-2 value counts that produce
   * partial (tail) vectors. Tests the overhead of flush logic for short vectors.
   */
  @State(Scope.Benchmark)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 5, time = 3)
  @Fork(value = 2)
  public static class PartialVector {

    /** Value counts chosen to exercise tail handling:
     * 1025 = 1 full + 1 value, 2000 = 1 full + 976, 5000 = 4 full + 904,
     * 65000 = 63 full + 488 */
    @Param({"1025", "2000", "5000", "65000"})
    private int numValues;

    private float[] floatData;
    private double[] doubleData;
    private AlpValuesWriter.FloatAlpValuesWriter floatWriter;
    private AlpValuesWriter.DoubleAlpValuesWriter doubleWriter;

    @Setup(Level.Trial)
    public void setup() {
      Random rand = new Random(42);
      floatData = generateFloatData(DataDistribution.MONETARY, numValues, rand);
      doubleData = generateDoubleData(DataDistribution.MONETARY, numValues, rand);
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
}
