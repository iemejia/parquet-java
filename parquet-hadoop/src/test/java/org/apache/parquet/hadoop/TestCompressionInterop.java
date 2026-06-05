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
package org.apache.parquet.hadoop;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end round-trip tests for all supported compression codecs.
 *
 * <p>These tests verify that data written with each codec can be read back
 * correctly, including multi-row-group scenarios.
 */
public class TestCompressionInterop {

  private static final Logger LOG = LoggerFactory.getLogger(TestCompressionInterop.class);

  private static final int PAGE_SIZE = 64 * 1024;
  private static final int ROW_GROUP_SIZE = 256 * 1024;
  private static final int NUM_RECORDS = 500;

  private static final MessageType SCHEMA = parseMessageType("message test { "
      + "required binary binary_field; "
      + "required int32 int32_field; "
      + "required int64 int64_field; "
      + "required boolean boolean_field; "
      + "required float float_field; "
      + "required double double_field; "
      + "required fixed_len_byte_array(3) flba_field; "
      + "required int96 int96_field; "
      + "} ");

  /**
   * All codecs that have a direct implementation in CodecFactory.
   */
  private static final CompressionCodecName[] ALL_CODECS = {
    CompressionCodecName.SNAPPY,
    CompressionCodecName.GZIP,
    CompressionCodecName.ZSTD,
    CompressionCodecName.LZ4_RAW,
    CompressionCodecName.LZO,
    CompressionCodecName.BROTLI,
  };

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  // ---- Round-trip tests for each codec ----

  @Test
  public void roundTrip_SNAPPY() throws Exception {
    testRoundTrip(CompressionCodecName.SNAPPY);
  }

  @Test
  public void roundTrip_GZIP() throws Exception {
    testRoundTrip(CompressionCodecName.GZIP);
  }

  @Test
  public void roundTrip_ZSTD() throws Exception {
    testRoundTrip(CompressionCodecName.ZSTD);
  }

  @Test
  public void roundTrip_LZ4_RAW() throws Exception {
    testRoundTrip(CompressionCodecName.LZ4_RAW);
  }

  @Test
  public void roundTrip_LZO() throws Exception {
    testRoundTrip(CompressionCodecName.LZO);
  }

  @Test
  public void roundTrip_BROTLI() throws Exception {
    testRoundTrip(CompressionCodecName.BROTLI);
  }

  // ---- All codecs in one test ----

  @Test
  public void roundTripAllCodecs() throws Exception {
    for (CompressionCodecName codec : ALL_CODECS) {
      LOG.info("Testing round-trip for codec: {}", codec);
      testRoundTrip(codec);
    }
  }

  // ---- Multi-row-group test to validate compression across row group boundaries ----

  @Test
  public void roundTrip_multiRowGroup() throws Exception {
    for (CompressionCodecName codec : ALL_CODECS) {
      LOG.info("Testing multi-row-group round-trip for codec: {}", codec);
      testRoundTripMultiRowGroup(codec);
    }
  }

  // ---- Implementation ----

  private void testRoundTrip(CompressionCodecName codec) throws Exception {
    Configuration conf = new Configuration();
    Path file = tempFolder.newFolder().toPath().resolve("roundtrip_" + codec.name() + ".parquet");

    // Write
    CodecFactory factory = new CodecFactory(conf, PAGE_SIZE);
    List<Group> expectedRecords = writeFile(file, codec, factory);
    factory.release();

    // Read
    CodecFactory readFactory = new CodecFactory(conf, PAGE_SIZE);
    List<Group> actualRecords = readFile(file, readFactory);
    readFactory.release();

    // Verify
    assertRecordsEqual(expectedRecords, actualRecords, codec.name() + " round-trip");
  }

  private void testRoundTripMultiRowGroup(CompressionCodecName codec) throws Exception {
    Configuration conf = new Configuration();
    Path file = tempFolder.newFolder().toPath().resolve("roundtrip_mrg_" + codec.name() + ".parquet");

    // Use a small row group size to force multiple row groups
    int smallRowGroupSize = 4 * 1024;

    CodecFactory writeFactory = new CodecFactory(conf, PAGE_SIZE);
    List<Group> expectedRecords = writeFile(file, codec, writeFactory, smallRowGroupSize, 1000);
    writeFactory.release();

    CodecFactory readFactory = new CodecFactory(conf, PAGE_SIZE);
    List<Group> actualRecords = readFile(file, readFactory);
    readFactory.release();

    assertRecordsEqual(expectedRecords, actualRecords, codec.name() + " multi-row-group round-trip");
  }

  private List<Group> writeFile(Path file, CompressionCodecName codec, CompressionCodecFactory factory)
      throws IOException {
    return writeFile(file, codec, factory, ROW_GROUP_SIZE, NUM_RECORDS);
  }

  private List<Group> writeFile(
      Path file, CompressionCodecName codec, CompressionCodecFactory factory, int rowGroupSize, int numRecords)
      throws IOException {
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
    List<Group> records = generateRecords(groupFactory, numRecords);

    OutputFile outputFile = new LocalOutputFile(file);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
        .withType(SCHEMA)
        .withCompressionCodec(codec)
        .withCodecFactory(factory)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(PAGE_SIZE)
        .withWriteMode(ParquetFileWriter.Mode.CREATE)
        .build()) {
      for (Group record : records) {
        writer.write(record);
      }
    }

    return records;
  }

  private List<Group> readFile(Path file, CompressionCodecFactory factory) throws IOException {
    List<Group> records = new ArrayList<>();
    InputFile inputFile = new LocalInputFile(file);
    try (ParquetReader<Group> reader = new ParquetReader.Builder<Group>(inputFile) {
      @Override
      protected ReadSupport<Group> getReadSupport() {
        return new GroupReadSupport();
      }
    }.withCodecFactory(factory).build()) {
      Group record;
      while ((record = reader.read()) != null) {
        records.add(record);
      }
    }
    return records;
  }

  private List<Group> generateRecords(SimpleGroupFactory factory, int numRecords) {
    List<Group> records = new ArrayList<>(numRecords);
    Random random = new Random(42); // fixed seed for reproducibility

    for (int i = 0; i < numRecords; i++) {
      byte[] binaryData = new byte[10 + random.nextInt(50)];
      random.nextBytes(binaryData);

      byte[] flbaData = new byte[3];
      random.nextBytes(flbaData);

      byte[] int96Data = new byte[12];
      random.nextBytes(int96Data);

      records.add(factory.newGroup()
          .append("binary_field", Binary.fromConstantByteArray(binaryData))
          .append("int32_field", random.nextInt())
          .append("int64_field", random.nextLong())
          .append("boolean_field", random.nextBoolean())
          .append("float_field", random.nextFloat())
          .append("double_field", random.nextDouble())
          .append("flba_field", Binary.fromConstantByteArray(flbaData))
          .append("int96_field", Binary.fromConstantByteArray(int96Data)));
    }
    return records;
  }

  private void assertRecordsEqual(List<Group> expected, List<Group> actual, String context) {
    assertEquals("Record count mismatch for " + context, expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Group exp = expected.get(i);
      Group act = actual.get(i);
      String msg = context + " record " + i;

      assertArrayEquals(
          msg + " binary_field",
          exp.getBinary("binary_field", 0).getBytes(),
          act.getBinary("binary_field", 0).getBytes());
      assertEquals(msg + " int32_field", exp.getInteger("int32_field", 0), act.getInteger("int32_field", 0));
      assertEquals(msg + " int64_field", exp.getLong("int64_field", 0), act.getLong("int64_field", 0));
      assertEquals(
          msg + " boolean_field", exp.getBoolean("boolean_field", 0), act.getBoolean("boolean_field", 0));
      assertEquals(msg + " float_field", exp.getFloat("float_field", 0), act.getFloat("float_field", 0), 0.0f);
      assertEquals(
          msg + " double_field", exp.getDouble("double_field", 0), act.getDouble("double_field", 0), 0.0d);
      assertArrayEquals(
          msg + " flba_field",
          exp.getBinary("flba_field", 0).getBytes(),
          act.getBinary("flba_field", 0).getBytes());
      assertArrayEquals(
          msg + " int96_field",
          exp.getInt96("int96_field", 0).getBytes(),
          act.getInt96("int96_field", 0).getBytes());
    }
  }
}
