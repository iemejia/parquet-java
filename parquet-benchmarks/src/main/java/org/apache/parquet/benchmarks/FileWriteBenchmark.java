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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
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

/**
 * File-level write benchmarks measuring end-to-end Parquet write throughput including
 * filesystem I/O through the example {@link Group} API. Row contents are pre-generated
 * during setup so the timed section covers encoding, compression, and local-disk writes.
 *
 * <p>Writes go to a temporary file via {@link LocalOutputFile}. Each invocation
 * overwrites the same file so the results include real filesystem cost (page-cache
 * writes, metadata syncs). For CPU-only benchmarks that isolate encoding and compression
 * cost from I/O, see {@link CpuWriteBenchmark}.
 *
 * <p>Parameterized across compression codec, writer version, dictionary encoding,
 * row-group block size, and data page size. Block size controls how many rows accumulate
 * before a row-group flush (triggering encoding, compression, and index generation).
 * Page size controls the unit of encoding and compression within a column chunk. Use JMH
 * {@code -p blockSize=...} and {@code -p pageSize=...} to select specific combinations
 * and avoid the full cross-product when not needed.
 *
 * <p>{@link Mode#SingleShotTime} is used because each invocation does enough work
 * (a full write of {@value TestDataFactory#DEFAULT_ROW_COUNT} rows) that JIT
 * amortization across invocations is unnecessary. Ten measurement iterations
 * provide stable statistics for SS mode.
 */
@BenchmarkMode(Mode.SingleShotTime)
@Fork(1)
@Warmup(iterations = 5, batchSize = 1)
@Measurement(iterations = 10, batchSize = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FileWriteBenchmark {

  @Param({"UNCOMPRESSED", "SNAPPY", "ZSTD", "GZIP", "LZ4_RAW", "BROTLI"})
  public String codec;

  @Param({"PARQUET_1_0", "PARQUET_2_0"})
  public String writerVersion;

  @Param({"true", "false"})
  public String dictionary;

  // Row-group block size in bytes: 128 MB (default), 256 MB (common production), 512 MB (stress)
  @Param({"134217728", "268435456", "536870912"})
  public int blockSize;

  // Data page size in bytes: 1 MB (default), 4 MB (reduced overhead), 8 MB (max throughput)
  @Param({"1048576", "4194304", "8388608"})
  public int pageSize;

  private Group[] rows;
  private File tempFile;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    rows = TestDataFactory.generateRows(
        TestDataFactory.newGroupFactory(), TestDataFactory.DEFAULT_ROW_COUNT, TestDataFactory.DEFAULT_SEED);
    tempFile = File.createTempFile("parquet-write-bench-", ".parquet");
    tempFile.deleteOnExit();
    tempFile.delete(); // remove so the writer can create it
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (tempFile != null && tempFile.exists()) {
      tempFile.delete();
    }
  }

  @Benchmark
  public void writeFile() throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(tempFile.toPath()))
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(TestDataFactory.FILE_BENCHMARK_SCHEMA)
        .withCompressionCodec(CompressionCodecName.valueOf(codec))
        .withWriterVersion(WriterVersion.valueOf(writerVersion))
        .withDictionaryEncoding(Boolean.parseBoolean(dictionary))
        .withRowGroupSize(blockSize)
        .withPageSize(pageSize)
        .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
  }
}
