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
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark measuring row group flush performance and memory usage.
 *
 * <p>Writes enough rows to trigger at least one row group flush, using a
 * {@link BlackHoleOutputFile} to isolate the flush cost from filesystem I/O.
 * Reports both wall-clock time and peak heap memory used during the write.
 *
 * <p>The row group size is set deliberately small (8MB) so that multiple
 * flushes occur within a single benchmark invocation, making the flush
 * overhead a significant fraction of the total time.
 */
@BenchmarkMode({Mode.SingleShotTime, Mode.AverageTime})
@Fork(value = 1, jvmArgs = {"-Xms256m", "-Xmx512m"})
@Warmup(iterations = 3, batchSize = 1)
@Measurement(iterations = 5, batchSize = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class RowGroupFlushBenchmark {

  /** Number of rows to write -- enough to trigger multiple row group flushes at 8MB row groups. */
  private static final int ROW_COUNT = 500_000;

  /** Small row group size to trigger multiple flushes per benchmark invocation. */
  private static final int ROW_GROUP_SIZE = 8 * 1024 * 1024; // 8 MB

  @Param({"UNCOMPRESSED", "SNAPPY"})
  public String codec;

  @Param({"PARQUET_1_0", "PARQUET_2_0"})
  public String writerVersion;

  /**
   * Auxiliary counters reported alongside the benchmark timing.
   * JMH collects these after each iteration.
   */
  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class MemoryCounters {
    /** Peak heap memory used (bytes) during the benchmark iteration. */
    public long peakMemoryUsedBytes;

    @Setup(Level.Iteration)
    public void reset() {
      peakMemoryUsedBytes = 0;
    }
  }

  @Benchmark
  public void writeWithFlush(MemoryCounters counters) throws IOException {
    // Force GC before measurement for a clean baseline
    System.gc();
    Runtime runtime = Runtime.getRuntime();
    long baselineUsed = runtime.totalMemory() - runtime.freeMemory();
    long peakUsed = baselineUsed;

    SimpleGroupFactory factory = TestDataFactory.newGroupFactory();
    Random random = new Random(42);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BlackHoleOutputFile.INSTANCE)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(TestDataFactory.FILE_BENCHMARK_SCHEMA)
        .withCompressionCodec(CompressionCodecName.valueOf(codec))
        .withWriterVersion(WriterVersion.valueOf(writerVersion))
        .withRowGroupSize(ROW_GROUP_SIZE)
        .withDictionaryEncoding(true)
        .build()) {
      for (int i = 0; i < ROW_COUNT; i++) {
        writer.write(TestDataFactory.generateRow(factory, i, random));
        // Sample memory periodically (every 10K rows) to track peak
        if (i % 10_000 == 0) {
          long currentUsed = runtime.totalMemory() - runtime.freeMemory();
          if (currentUsed > peakUsed) {
            peakUsed = currentUsed;
          }
        }
      }
    }

    counters.peakMemoryUsedBytes = peakUsed - baselineUsed;
  }
}
