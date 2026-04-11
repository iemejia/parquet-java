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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * JUnit correctness test that verifies data integrity when multiple threads write and
 * read Parquet files concurrently. This is <em>not</em> a benchmark — it validates that
 * no data corruption occurs under contention.
 *
 * <p>Eight threads each write a separate file, then read it back and verify every row
 * matches the expected values.
 */
public class ConcurrentCorrectnessTest {

  private static final int THREAD_COUNT = 8;
  private static final int ROWS_PER_THREAD = 10_000;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void concurrentWriteThenReadPreservesData() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<Void>> futures = new ArrayList<>();

    for (int t = 0; t < THREAD_COUNT; t++) {
      final int threadIndex = t;
      futures.add(executor.submit(() -> {
        startLatch.await(); // all threads start together

        File file = new File(tempFolder.getRoot(), "thread-" + threadIndex + ".parquet");
        writeFile(file, threadIndex);
        readAndVerifyFile(file, threadIndex);
        return null;
      }));
    }

    // Release all threads simultaneously
    startLatch.countDown();

    // Collect results (will rethrow any assertion errors)
    for (Future<Void> future : futures) {
      future.get(60, TimeUnit.SECONDS);
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
  }

  private void writeFile(File file, int threadIndex) throws IOException {
    SimpleGroupFactory factory = TestDataFactory.newGroupFactory();
    Random random = new Random(threadIndex); // deterministic per thread
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(
            new LocalOutputFile(file.toPath()))
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(TestDataFactory.FILE_BENCHMARK_SCHEMA)
        .build()) {
      for (int i = 0; i < ROWS_PER_THREAD; i++) {
        writer.write(TestDataFactory.generateRow(factory, i, random));
      }
    }
  }

  private void readAndVerifyFile(File file, int threadIndex) throws IOException {
    Random expectedRandom = new Random(threadIndex); // same seed as write
    int rowCount = 0;

    InputFile inputFile = new LocalInputFile(file.toPath());
    try (ParquetReader<Group> reader = new ParquetReader.Builder<Group>(inputFile) {
      @Override
      protected ReadSupport<Group> getReadSupport() {
        return new GroupReadSupport();
      }
    }.build()) {
      Group group;
      while ((group = reader.read()) != null) {
        int i = rowCount;

        // Verify deterministic fields
        assertEquals("int32_field mismatch at row " + i, i, group.getInteger("int32_field", 0));
        assertEquals("int64_field mismatch at row " + i, (long) i * 100, group.getLong("int64_field", 0));
        assertEquals("boolean_field mismatch at row " + i, i % 2 == 0, group.getBoolean("boolean_field", 0));
        assertEquals(
            "binary_field mismatch at row " + i,
            "value_" + (i % 1000),
            group.getBinary("binary_field", 0).toStringUsingUTF8());

        // Verify random fields match the same seed sequence
        float expectedFloat = expectedRandom.nextFloat();
        double expectedDouble = expectedRandom.nextDouble();
        assertEquals("float_field mismatch at row " + i, expectedFloat, group.getFloat("float_field", 0), 0.0f);
        assertEquals(
            "double_field mismatch at row " + i, expectedDouble, group.getDouble("double_field", 0), 0.0);

        rowCount++;
      }
    }

    assertEquals(
        "Row count mismatch for thread " + threadIndex,
        ROWS_PER_THREAD,
        rowCount);
  }
}
