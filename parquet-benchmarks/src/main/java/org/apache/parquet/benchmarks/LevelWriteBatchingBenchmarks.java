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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.openjdk.jmh.annotations.Mode.SingleShotTime;
import static org.openjdk.jmh.annotations.Scope.Benchmark;

import java.io.IOException;
import java.util.Random;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark for the level write batching optimization in ColumnWriterBase.
 * <p>
 * Measures the throughput of writing records with various nesting depths and data patterns
 * that exercise the repetition/definition level encoding path. The optimization buffers
 * levels internally and flushes in batch to the RLE encoder's writeIntegers() method,
 * avoiding per-value virtual dispatch overhead and enabling the encoder's run-scanning
 * optimization.
 * <p>
 * Writes to a black-hole OutputFile to isolate the write-path performance from I/O.
 *
 * <pre>
 * ./mvnw clean package -pl parquet-benchmarks -am -DskipTests &amp;&amp; \
 *   java -jar parquet-benchmarks/target/parquet-benchmarks.jar \
 *     org.apache.parquet.benchmarks.LevelWriteBatchingBenchmarks -rf json
 * </pre>
 */
@BenchmarkMode(SingleShotTime)
@Fork(1)
@Warmup(iterations = 10, batchSize = 1)
@Measurement(iterations = 50, batchSize = 1)
@OutputTimeUnit(MILLISECONDS)
@State(Benchmark)
public class LevelWriteBatchingBenchmarks {

  private static final int RECORD_COUNT = 10_000_000;

  private static final OutputFile BLACK_HOLE = new OutputFile() {
    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return -1L;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new PositionOutputStream() {
        private long pos;

        @Override
        public long getPos() throws IOException {
          return pos;
        }

        @Override
        public void write(int b) throws IOException {
          ++pos;
        }
      };
    }

    @Override
    public String getPath() {
      throw new UnsupportedOperationException();
    }
  };

  // --- Flat schema: all rl=0, dl=0 (best case for RLE: long runs of identical levels) ---

  private static final MessageType FLAT_SCHEMA = Types.buildMessage()
      .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
      .named("value")
      .named("msg");

  /**
   * Baseline: flat required field, all levels are 0.
   * This exercises the level batching with the simplest possible pattern
   * (long runs of identical 0 values -- best case for RLE batch optimization).
   */
  @Benchmark
  public void writeFlatRequired() throws IOException {
    GroupFactory factory = new SimpleGroupFactory(FLAT_SCHEMA);
    Random random = new Random(42);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BLACK_HOLE)
        .withWriteMode(Mode.OVERWRITE)
        .withWriterVersion(PARQUET_2_0)
        .withType(FLAT_SCHEMA)
        .build()) {
      for (int i = 0; i < RECORD_COUNT; i++) {
        Group group = factory.newGroup();
        group.append("value", random.nextInt());
        writer.write(group);
      }
    }
  }

  // --- Optional flat: rl=0, dl varies (0 or 1) ---

  private static final MessageType OPTIONAL_FLAT_SCHEMA = Types.buildMessage()
      .optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
      .named("value")
      .named("msg");

  /**
   * Optional flat field with mixed nulls (50% null).
   * Definition levels alternate between 0 and 1 -- tests batching with varied dl values.
   */
  @Benchmark
  public void writeOptionalFlat50PercentNull() throws IOException {
    GroupFactory factory = new SimpleGroupFactory(OPTIONAL_FLAT_SCHEMA);
    Random random = new Random(42);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BLACK_HOLE)
        .withWriteMode(Mode.OVERWRITE)
        .withWriterVersion(PARQUET_2_0)
        .withType(OPTIONAL_FLAT_SCHEMA)
        .build()) {
      for (int i = 0; i < RECORD_COUNT; i++) {
        Group group = factory.newGroup();
        if (random.nextBoolean()) {
          group.append("value", random.nextInt());
        }
        writer.write(group);
      }
    }
  }

  // --- Nested list schema: repeated fields with varying rl/dl ---

  private static final MessageType NESTED_LIST_SCHEMA = Types.buildMessage()
      .optionalList()
      .optionalElement(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
      .named("int_list")
      .named("msg");

  /**
   * Nested optional list with variable-length lists (1-5 elements).
   * Exercises mixed repetition levels (0 for first element, 1 for subsequent)
   * and varying definition levels. This is the primary target for the batching
   * optimization since it generates the most diverse level patterns.
   */
  @Benchmark
  public void writeNestedList() throws IOException {
    GroupFactory factory = new SimpleGroupFactory(NESTED_LIST_SCHEMA);
    Random random = new Random(42);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BLACK_HOLE)
        .withWriteMode(Mode.OVERWRITE)
        .withWriterVersion(PARQUET_2_0)
        .withType(NESTED_LIST_SCHEMA)
        .build()) {
      for (int i = 0; i < RECORD_COUNT; i++) {
        Group group = factory.newGroup();
        if (random.nextDouble() > 0.1) { // 90% have a list
          Group list = group.addGroup("int_list");
          int listSize = 1 + random.nextInt(5);
          for (int j = 0; j < listSize; j++) {
            list.addGroup("list").append("element", random.nextInt());
          }
        }
        writer.write(group);
      }
    }
  }

  // --- Deeply nested schema: higher max rl/dl ---

  private static final MessageType DEEP_NESTED_SCHEMA = Types.buildMessage()
      .optionalList()
      .optionalListElement()
      .optionalElement(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
      .named("nested_list")
      .named("msg");

  /**
   * Deeply nested schema (list of lists) with high max repetition/definition levels.
   * Generates the most diverse level patterns and highest per-value overhead,
   * making the batching optimization most impactful.
   */
  @Benchmark
  public void writeDeepNested() throws IOException {
    GroupFactory factory = new SimpleGroupFactory(DEEP_NESTED_SCHEMA);
    Random random = new Random(42);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BLACK_HOLE)
        .withWriteMode(Mode.OVERWRITE)
        .withWriterVersion(PARQUET_2_0)
        .withType(DEEP_NESTED_SCHEMA)
        .build()) {
      for (int i = 0; i < RECORD_COUNT; i++) {
        Group group = factory.newGroup();
        if (random.nextDouble() > 0.1) { // 90% have an outer list
          Group outerList = group.addGroup("nested_list");
          int outerSize = 1 + random.nextInt(3);
          for (int j = 0; j < outerSize; j++) {
            Group innerList = outerList.addGroup("list").addGroup("element");
            int innerSize = 1 + random.nextInt(4);
            for (int k = 0; k < innerSize; k++) {
              innerList.addGroup("list").append("element", random.nextInt());
            }
          }
        }
        writer.write(group);
      }
    }
  }

  // --- Mostly-null nested (99% null) -- exercises long runs of identical levels ---

  private static final MessageType MOSTLY_NULL_NESTED_SCHEMA = Types.buildMessage()
      .optionalList()
      .optionalElement(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
      .named("int_list")
      .optionalList()
      .optionalListElement()
      .optionalElement(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
      .named("dummy_list")
      .named("msg");

  /**
   * Mostly-null nested schema (99% null records). Generates long runs of identical
   * level values (rl=0, dl=0), which is the best case for the RLE encoder's batch
   * run-scanning optimization enabled by level write batching.
   */
  @Benchmark
  public void writeMostlyNullNested() throws IOException {
    GroupFactory factory = new SimpleGroupFactory(MOSTLY_NULL_NESTED_SCHEMA);
    Random random = new Random(42);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BLACK_HOLE)
        .withWriteMode(Mode.OVERWRITE)
        .withWriterVersion(PARQUET_2_0)
        .withType(MOSTLY_NULL_NESTED_SCHEMA)
        .build()) {
      for (int i = 0; i < RECORD_COUNT; i++) {
        Group group = factory.newGroup();
        if (random.nextDouble() > 0.99) {
          Group list = group.addGroup("int_list");
          list.addGroup("list").append("element", random.nextInt());
        }
        writer.write(group);
      }
    }
  }
}
