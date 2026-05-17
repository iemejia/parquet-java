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
package org.apache.parquet.column.impl;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.parquet.Version;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.mem.MemPageReader;
import org.apache.parquet.column.page.mem.MemPageWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

/**
 * Tests for the level write batching optimization in {@link ColumnWriterBase}.
 * <p>
 * The optimization buffers repetition and definition levels internally and flushes them
 * in batch to the RLE encoder's writeIntegers() method, instead of encoding one level
 * at a time. These tests verify that the batching produces the same results as the
 * previous one-at-a-time approach across various scenarios.
 */
public class TestColumnWriterLevelBatching {

  /**
   * Tests writing exactly LEVEL_BUFFER_SIZE values to trigger exactly one buffer flush,
   * plus one more to start the next buffer.
   */
  @Test
  public void testExactBufferBoundaryV2() throws Exception {
    int count = ColumnWriterBase.LEVEL_BUFFER_SIZE + 1;
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 id; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(1024 * 1024) // Large page to avoid mid-test page breaks
            .build());

    for (int i = 0; i < count; i++) {
      writer.write(i, 0, 0);
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals("Should produce exactly 1 page", 1, pages.size());
    DataPageV2 page = (DataPageV2) pages.get(0);
    assertEquals("Row count should match", count, page.getRowCount());
    assertEquals("Value count should match", count, page.getValueCount());

    // Read back and verify all values
    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < count; i++) {
      assertEquals("rl mismatch at " + i, 0, columnReader.getCurrentRepetitionLevel());
      assertEquals("dl mismatch at " + i, 0, columnReader.getCurrentDefinitionLevel());
      columnReader.writeCurrentValueToConverter();
      assertEquals("value mismatch at " + i, i, collector.lastValue);
      columnReader.consume();
    }
  }

  /**
   * Tests writing with non-zero repetition levels (nested schema) to verify that
   * row counting via rl==0 scanning in the buffer flush is correct.
   */
  @Test
  public void testNestedSchemaWithRepetitionLevelsV2() throws Exception {
    // Schema: message test { repeated group list { required int32 element; } }
    // This gives us: maxRepetitionLevel=1, maxDefinitionLevel=1
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { repeated group list { required int32 element; } }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(1024 * 1024)
            .build());

    // Write 5 records:
    // Record 0: [10, 20, 30]       -> (rl=0,dl=1), (rl=1,dl=1), (rl=1,dl=1)
    // Record 1: [40]               -> (rl=0,dl=1)
    // Record 2: [50, 60]           -> (rl=0,dl=1), (rl=1,dl=1)
    // Record 3: [70, 80, 90, 100]  -> (rl=0,dl=1), (rl=1,dl=1), (rl=1,dl=1), (rl=1,dl=1)
    // Record 4: [110]              -> (rl=0,dl=1)
    int[][] records = {{10, 20, 30}, {40}, {50, 60}, {70, 80, 90, 100}, {110}};
    int totalValues = 0;
    for (int[] record : records) {
      for (int j = 0; j < record.length; j++) {
        int rl = (j == 0) ? 0 : 1;
        writer.write(record[j], rl, 1);
        totalValues++;
      }
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals(1, pages.size());
    DataPageV2 page = (DataPageV2) pages.get(0);
    assertEquals("Should have 5 rows (records)", 5, page.getRowCount());
    assertEquals("Should have 11 values total", totalValues, page.getValueCount());

    // Read back and verify
    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));

    int valueIdx = 0;
    for (int[] record : records) {
      for (int j = 0; j < record.length; j++) {
        int expectedRl = (j == 0) ? 0 : 1;
        assertEquals("rl mismatch at value " + valueIdx, expectedRl, columnReader.getCurrentRepetitionLevel());
        assertEquals("dl mismatch at value " + valueIdx, 1, columnReader.getCurrentDefinitionLevel());
        columnReader.writeCurrentValueToConverter();
        assertEquals("value mismatch at value " + valueIdx, record[j], collector.lastValue);
        columnReader.consume();
        valueIdx++;
      }
    }
  }

  /**
   * Tests that writing across multiple buffer flushes with nested levels produces
   * correct results. Uses more values than LEVEL_BUFFER_SIZE to force multiple flushes.
   */
  @Test
  public void testMultipleBufferFlushesWithNestedLevelsV2() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { repeated group list { required int32 element; } }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(4 * 1024 * 1024)
            .build());

    // Write enough data to trigger multiple buffer flushes:
    // Each record has 3 values -> ~3x LEVEL_BUFFER_SIZE values
    int numRecords = ColumnWriterBase.LEVEL_BUFFER_SIZE;
    int valuesPerRecord = 3;
    int totalValues = numRecords * valuesPerRecord;

    for (int r = 0; r < numRecords; r++) {
      for (int v = 0; v < valuesPerRecord; v++) {
        int rl = (v == 0) ? 0 : 1;
        writer.write(r * valuesPerRecord + v, rl, 1);
      }
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals(1, pages.size());
    DataPageV2 page = (DataPageV2) pages.get(0);
    assertEquals("Row count should be " + numRecords, numRecords, page.getRowCount());
    assertEquals("Value count should be " + totalValues, totalValues, page.getValueCount());

    // Read back and verify all values
    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));

    for (int r = 0; r < numRecords; r++) {
      for (int v = 0; v < valuesPerRecord; v++) {
        int expectedRl = (v == 0) ? 0 : 1;
        assertEquals(expectedRl, columnReader.getCurrentRepetitionLevel());
        assertEquals(1, columnReader.getCurrentDefinitionLevel());
        columnReader.writeCurrentValueToConverter();
        assertEquals(r * valuesPerRecord + v, collector.lastValue);
        columnReader.consume();
      }
    }
  }

  /**
   * Tests writing nulls with optional fields to verify definition levels are
   * correctly batched and flushed.
   */
  @Test
  public void testOptionalNullsV2() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { optional int32 value; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(1024 * 1024)
            .build());

    // Write 2 * LEVEL_BUFFER_SIZE values: alternating null and non-null
    int count = 2 * ColumnWriterBase.LEVEL_BUFFER_SIZE;
    for (int i = 0; i < count; i++) {
      if (i % 2 == 0) {
        writer.writeNull(0, 0); // null (dl=0 for optional, meaning not defined)
      } else {
        writer.write(i, 0, 1); // non-null (dl=1 for optional, meaning defined)
      }
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals(1, pages.size());
    DataPageV2 page = (DataPageV2) pages.get(0);
    assertEquals(count, page.getRowCount());
    assertEquals(count, page.getValueCount());
    assertEquals(count / 2, page.getNullCount());

    // Read back and verify
    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < count; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      if (i % 2 == 0) {
        assertEquals("dl for null at " + i, 0, columnReader.getCurrentDefinitionLevel());
      } else {
        assertEquals("dl for non-null at " + i, 1, columnReader.getCurrentDefinitionLevel());
        columnReader.writeCurrentValueToConverter();
        assertEquals(i, collector.lastValue);
      }
      columnReader.consume();
    }
  }

  /**
   * Tests multiple page writes to verify that the buffer is correctly reset
   * between pages and row counts accumulate properly.
   */
  @Test
  public void testMultiplePagesV2() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { repeated group list { required int32 element; } }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(1024 * 1024)
            .build());

    int recordsPerPage = 500;
    int numPages = 5;
    int valuesPerRecord = 2; // [x, y] per record

    for (int p = 0; p < numPages; p++) {
      for (int r = 0; r < recordsPerPage; r++) {
        int base = (p * recordsPerPage + r) * valuesPerRecord;
        writer.write(base, 0, 1);
        writer.write(base + 1, 1, 1);
      }
      writer.writePage();
    }
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals(numPages, pages.size());

    int totalValues = 0;
    int totalRows = 0;
    for (DataPage page : pages) {
      DataPageV2 v2 = (DataPageV2) page;
      assertEquals(recordsPerPage, v2.getRowCount());
      assertEquals(recordsPerPage * valuesPerRecord, v2.getValueCount());
      totalValues += v2.getValueCount();
      totalRows += v2.getRowCount();
    }
    assertEquals(numPages * recordsPerPage, totalRows);
    assertEquals(numPages * recordsPerPage * valuesPerRecord, totalValues);

    // Read back and verify all values across all pages
    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));

    for (int p = 0; p < numPages; p++) {
      for (int r = 0; r < recordsPerPage; r++) {
        int base = (p * recordsPerPage + r) * valuesPerRecord;
        assertEquals(0, columnReader.getCurrentRepetitionLevel());
        assertEquals(1, columnReader.getCurrentDefinitionLevel());
        columnReader.writeCurrentValueToConverter();
        assertEquals(base, collector.lastValue);
        columnReader.consume();

        assertEquals(1, columnReader.getCurrentRepetitionLevel());
        assertEquals(1, columnReader.getCurrentDefinitionLevel());
        columnReader.writeCurrentValueToConverter();
        assertEquals(base + 1, collector.lastValue);
        columnReader.consume();
      }
    }
  }

  /**
   * Tests the V1 writer variant (which uses different page encoding) to ensure
   * level batching works across both page format versions.
   */
  @Test
  public void testV1WriterWithBatching() throws Exception {
    int count = ColumnWriterBase.LEVEL_BUFFER_SIZE * 2 + 100;
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary name; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV1 writer = new ColumnWriterV1(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_1_0)
            .withPageSize(1024 * 1024)
            .build());

    for (int i = 0; i < count; i++) {
      writer.write(Binary.fromString("val" + i), 0, 0);
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals(1, pages.size());
    assertEquals(count, pages.get(0).getValueCount());

    // Read back and verify
    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    BinaryCollector collector = new BinaryCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));

    for (int i = 0; i < count; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      assertEquals(0, columnReader.getCurrentDefinitionLevel());
      columnReader.writeCurrentValueToConverter();
      assertEquals("val" + i, collector.lastValue.toStringUsingUTF8());
      columnReader.consume();
    }
  }

  /**
   * Tests writing with deeper nesting (maxRepetitionLevel=2) to verify batching
   * handles multiple non-zero repetition levels correctly.
   */
  @Test
  public void testDeeplyNestedSchemaV2() throws Exception {
    // A list of lists: maxRep=2, maxDef=2
    MessageType schema = MessageTypeParser.parseMessageType("message test { "
        + "repeated group outer_list { "
        + "  repeated group inner_list { "
        + "    required int32 value; "
        + "  } "
        + "} "
        + "}");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(1024 * 1024)
            .build());

    // Write 3 records:
    // Record 0: [[1,2],[3]]        -> (0,2),(2,2),(1,2)
    // Record 1: [[4]]              -> (0,2)
    // Record 2: [[5,6,7],[8,9]]    -> (0,2),(2,2),(2,2),(1,2),(2,2)
    int[][] rls = {{0, 2, 1}, {0}, {0, 2, 2, 1, 2}};
    int[][] vals = {{1, 2, 3}, {4}, {5, 6, 7, 8, 9}};

    int totalValues = 0;
    for (int r = 0; r < rls.length; r++) {
      for (int v = 0; v < rls[r].length; v++) {
        writer.write(vals[r][v], rls[r][v], 2);
        totalValues++;
      }
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals(1, pages.size());
    DataPageV2 page = (DataPageV2) pages.get(0);
    assertEquals("Should have 3 rows", 3, page.getRowCount());
    assertEquals(totalValues, page.getValueCount());

    // Read back and verify
    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));

    for (int r = 0; r < rls.length; r++) {
      for (int v = 0; v < rls[r].length; v++) {
        assertEquals(rls[r][v], columnReader.getCurrentRepetitionLevel());
        assertEquals(2, columnReader.getCurrentDefinitionLevel());
        columnReader.writeCurrentValueToConverter();
        assertEquals(vals[r][v], collector.lastValue);
        columnReader.consume();
      }
    }
  }

  /**
   * Tests writing fewer values than the buffer size (partial buffer) to verify
   * that the flush at writePage() correctly handles a partially-filled buffer.
   */
  @Test
  public void testPartialBufferFlushV2() throws Exception {
    int count = ColumnWriterBase.LEVEL_BUFFER_SIZE / 2;
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 id; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(1024 * 1024)
            .build());

    for (int i = 0; i < count; i++) {
      writer.write(i * 10, 0, 0);
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals(1, pages.size());
    DataPageV2 page = (DataPageV2) pages.get(0);
    assertEquals(count, page.getRowCount());
    assertEquals(count, page.getValueCount());

    // Read back and verify
    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));

    for (int i = 0; i < count; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      assertEquals(0, columnReader.getCurrentDefinitionLevel());
      columnReader.writeCurrentValueToConverter();
      assertEquals(i * 10, collector.lastValue);
      columnReader.consume();
    }
  }

  /**
   * Tests a single value to verify the minimal case works correctly.
   */
  @Test
  public void testSingleValueV2() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 id; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(1024 * 1024)
            .build());

    writer.write(42, 0, 0);
    writer.writePage();
    writer.finalizeColumnChunk();

    List<DataPage> pages = pageWriter.getPages();
    assertEquals(1, pages.size());
    DataPageV2 page = (DataPageV2) pages.get(0);
    assertEquals(1, page.getRowCount());
    assertEquals(1, page.getValueCount());

    MemPageReader reader =
        new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));
    assertEquals(0, columnReader.getCurrentRepetitionLevel());
    assertEquals(0, columnReader.getCurrentDefinitionLevel());
    columnReader.writeCurrentValueToConverter();
    assertEquals(42, collector.lastValue);
    columnReader.consume();
  }

  /**
   * Tests all value types (int, long, float, double, boolean, binary) to ensure
   * level batching works across all write() overloads.
   */
  @Test
  public void testAllValueTypesV2() throws Exception {
    int count = ColumnWriterBase.LEVEL_BUFFER_SIZE + 50;

    // Test int
    verifyWriteReadInt(count);
    // Test long
    verifyWriteReadLong(count);
    // Test float
    verifyWriteReadFloat(count);
    // Test double
    verifyWriteReadDouble(count);
    // Test boolean
    verifyWriteReadBoolean(count);
  }

  private void verifyWriteReadInt(int count) throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 v; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(4 * 1024 * 1024)
            .build());
    for (int i = 0; i < count; i++) {
      writer.write(i, 0, 0);
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    MemPageReader reader = new MemPageReader(
        pageWriter.getTotalValueCount(), pageWriter.getPages().iterator(), pageWriter.getDictionaryPage());
    IntCollector collector = new IntCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < count; i++) {
      columnReader.writeCurrentValueToConverter();
      assertEquals(i, collector.lastValue);
      columnReader.consume();
    }
  }

  private void verifyWriteReadLong(int count) throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int64 v; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(4 * 1024 * 1024)
            .build());
    for (int i = 0; i < count; i++) {
      writer.write((long) i * 100L, 0, 0);
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    MemPageReader reader = new MemPageReader(
        pageWriter.getTotalValueCount(), pageWriter.getPages().iterator(), pageWriter.getDictionaryPage());
    LongCollector collector = new LongCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < count; i++) {
      columnReader.writeCurrentValueToConverter();
      assertEquals((long) i * 100L, collector.lastValue);
      columnReader.consume();
    }
  }

  private void verifyWriteReadFloat(int count) throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required float v; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(4 * 1024 * 1024)
            .build());
    for (int i = 0; i < count; i++) {
      writer.write((float) i * 0.5f, 0, 0);
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    MemPageReader reader = new MemPageReader(
        pageWriter.getTotalValueCount(), pageWriter.getPages().iterator(), pageWriter.getDictionaryPage());
    FloatCollector collector = new FloatCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < count; i++) {
      columnReader.writeCurrentValueToConverter();
      assertEquals((float) i * 0.5f, collector.lastValue, 0.0001f);
      columnReader.consume();
    }
  }

  private void verifyWriteReadDouble(int count) throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required double v; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(4 * 1024 * 1024)
            .build());
    for (int i = 0; i < count; i++) {
      writer.write((double) i * 1.5, 0, 0);
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    MemPageReader reader = new MemPageReader(
        pageWriter.getTotalValueCount(), pageWriter.getPages().iterator(), pageWriter.getDictionaryPage());
    DoubleCollector collector = new DoubleCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < count; i++) {
      columnReader.writeCurrentValueToConverter();
      assertEquals((double) i * 1.5, collector.lastValue, 0.0001);
      columnReader.consume();
    }
  }

  private void verifyWriteReadBoolean(int count) throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required boolean v; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(4 * 1024 * 1024)
            .build());
    for (int i = 0; i < count; i++) {
      writer.write(i % 2 == 0, 0, 0);
    }
    writer.writePage();
    writer.finalizeColumnChunk();

    MemPageReader reader = new MemPageReader(
        pageWriter.getTotalValueCount(), pageWriter.getPages().iterator(), pageWriter.getDictionaryPage());
    BooleanCollector collector = new BooleanCollector();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, reader, collector, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < count; i++) {
      columnReader.writeCurrentValueToConverter();
      assertEquals(i % 2 == 0, collector.lastValue);
      columnReader.consume();
    }
  }

  /**
   * Tests that rowsWrittenSoFar accumulates correctly across multiple pages
   * when level buffering is active.
   */
  @Test
  public void testRowsWrittenSoFarAccumulation() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { repeated group list { required int32 element; } }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 writer = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(1024 * 1024)
            .build());

    // Page 1: 100 records, 2 values each
    for (int r = 0; r < 100; r++) {
      writer.write(r, 0, 1);
      writer.write(r + 1000, 1, 1);
    }
    writer.writePage();
    assertEquals(100, writer.getRowsWrittenSoFar());

    // Page 2: 200 records, 1 value each
    for (int r = 0; r < 200; r++) {
      writer.write(r, 0, 1);
    }
    writer.writePage();
    assertEquals(300, writer.getRowsWrittenSoFar());

    writer.finalizeColumnChunk();
  }

  // Converter helpers for reading back values
  private static class IntCollector extends PrimitiveConverter {
    int lastValue;

    @Override
    public void addInt(int value) {
      lastValue = value;
    }
  }

  private static class LongCollector extends PrimitiveConverter {
    long lastValue;

    @Override
    public void addLong(long value) {
      lastValue = value;
    }
  }

  private static class FloatCollector extends PrimitiveConverter {
    float lastValue;

    @Override
    public void addFloat(float value) {
      lastValue = value;
    }
  }

  private static class DoubleCollector extends PrimitiveConverter {
    double lastValue;

    @Override
    public void addDouble(double value) {
      lastValue = value;
    }
  }

  private static class BooleanCollector extends PrimitiveConverter {
    boolean lastValue;

    @Override
    public void addBoolean(boolean value) {
      lastValue = value;
    }
  }

  private static class BinaryCollector extends PrimitiveConverter {
    Binary lastValue;

    @Override
    public void addBinary(Binary value) {
      lastValue = value;
    }
  }
}
