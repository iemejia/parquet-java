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
package org.apache.parquet.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Fuzz tests for the Parquet-Avro integration layer.
 *
 * <p>Exercises three critical hot paths in the parquet-avro module:</p>
 * <ol>
 *   <li>{@link AvroSchemaConverter}: Bidirectional conversion between Avro
 *       {@link Schema} and Parquet {@link MessageType}.</li>
 *   <li>Write/Read round-trip: Serializes an Avro {@link GenericRecord} through
 *       the full Parquet columnar format (shredding via AvroWriteSupport, then
 *       assembly via AvroReadSupport) using in-memory I/O.</li>
 *   <li>Parquet schema text parsing and conversion to Avro.</li>
 * </ol>
 */
public class ParquetAvroFuzzTest {

  private static final AvroSchemaConverter CONVERTER = new AvroSchemaConverter();

  /**
   * Schema using only types that round-trip cleanly through Parquet-Avro.
   * Avoids enums (stored as BINARY, read back as Utf8) and fixed
   * (FIXED_LEN_BYTE_ARRAY semantics differ).
   */
  private static final Schema PARQUET_SCHEMA = new Schema.Parser()
      .parse("{\"type\":\"record\",\"name\":\"ParquetRoot\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"long\"},"
          + "{\"name\":\"name\",\"type\":\"string\"},"
          + "{\"name\":\"active\",\"type\":\"boolean\"},"
          + "{\"name\":\"score\",\"type\":\"double\"},"
          + "{\"name\":\"payload\",\"type\":\"bytes\"},"
          + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
          + "{\"name\":\"counts\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},"
          + "{\"name\":\"choice\",\"type\":[\"null\",\"string\",\"long\"],\"default\":null},"
          + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"ParquetInner\",\"fields\":["
          + "{\"name\":\"x\",\"type\":\"int\"},{\"name\":\"y\",\"type\":\"int\"}]}}]}");

  /**
   * OSS-Fuzz entry point. Multiplexes between schema conversion, round-trip,
   * and Parquet schema parsing modes.
   */
  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    int mode = data.consumeInt(0, 2);
    try {
      switch (mode) {
        case 0:
          fuzzAvroToParquetSchemaConversion(data);
          break;
        case 1:
          fuzzParquetSchemaToAvroConversion(data);
          break;
        default:
          roundTripParquet(buildParquetRecord(data));
          break;
      }
    } catch (IOException e) {
      // Expected for I/O issues during write/read
    }
  }

  // --- JUnit @FuzzTest entry points ---

  @FuzzTest(maxDuration = "5m")
  public void fuzzAvroToParquetSchema(FuzzedDataProvider data) {
    fuzzAvroToParquetSchemaConversion(data);
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzParquetToAvroSchema(FuzzedDataProvider data) {
    fuzzParquetSchemaToAvroConversion(data);
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzParquetRoundTrip(FuzzedDataProvider data) throws IOException {
    roundTripParquet(buildParquetRecord(data));
  }

  // --- Shared logic ---

  /**
   * Converts a fuzz-generated Avro schema to Parquet MessageType and back.
   */
  private static void fuzzAvroToParquetSchemaConversion(FuzzedDataProvider data) {
    String schemaJson = buildAvroSchemaInput(data);
    Schema avroSchema;
    try {
      avroSchema = new Schema.Parser().parse(schemaJson);
    } catch (RuntimeException e) {
      // Invalid schema JSON
      return;
    }

    // Only record schemas can be converted to Parquet MessageType
    if (avroSchema.getType() != Schema.Type.RECORD) {
      return;
    }

    try {
      MessageType parquetSchema = CONVERTER.convert(avroSchema);
      // Convert back to Avro to test the reverse path
      Schema roundTripped = CONVERTER.convert(parquetSchema);
      if (roundTripped == null) {
        throw new AssertionError("AvroSchemaConverter.convert(MessageType) returned null");
      }
    } catch (RuntimeException e) {
      if (!isExpectedSchemaConversionFailure(e)) {
        throw e;
      }
    } catch (StackOverflowError e) {
      // Deeply nested/recursive schemas can overflow the stack
    }
  }

  /**
   * Parses fuzz-generated Parquet schema text and converts to Avro.
   */
  private static void fuzzParquetSchemaToAvroConversion(FuzzedDataProvider data) {
    String parquetSchemaStr = data.consumeRemainingAsString();
    try {
      MessageType messageType = MessageTypeParser.parseMessageType(parquetSchemaStr);
      Schema avroSchema = CONVERTER.convert(messageType);
      if (avroSchema == null) {
        throw new AssertionError("AvroSchemaConverter.convert(MessageType) returned null for valid MessageType");
      }
    } catch (RuntimeException e) {
      if (!isExpectedSchemaConversionFailure(e)) {
        throw e;
      }
    }
  }

  /**
   * Writes an Avro record to Parquet format in-memory and reads it back,
   * exercising the full columnar shredding/assembly pipeline.
   */
  private static void roundTripParquet(GenericRecord record) throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();

    // Write
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
        .withSchema(PARQUET_SCHEMA).withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(record);
    }

    byte[] parquetBytes = outputFile.toByteArray();

    // Read
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
        new InMemoryInputFile(parquetBytes)).build()) {
      GenericRecord decoded = reader.read();
      if (decoded == null) {
        throw new AssertionError("Parquet round-trip lost the encoded record");
      }
      if (!parquetRecordsEqual(record, decoded)) {
        throw new AssertionError("Parquet round-trip changed semantic value");
      }
      if (reader.read() != null) {
        throw new AssertionError("Parquet round-trip produced extra records");
      }
    }
  }

  private static boolean isExpectedSchemaConversionFailure(RuntimeException e) {
    return e instanceof IllegalArgumentException || e instanceof UnsupportedOperationException
        || e instanceof SchemaParseException || e instanceof NullPointerException
        || e instanceof ArrayIndexOutOfBoundsException;
  }

  // --- Record building and comparison ---

  private static GenericRecord buildParquetRecord(FuzzedDataProvider data) {
    GenericRecord record = new GenericData.Record(PARQUET_SCHEMA);
    record.put("id", data.consumeLong());
    record.put("name", safeString(data.consumeString(24), "parquet"));
    record.put("active", data.consumeBoolean());
    record.put("score", (data.consumeInt(-1000, 1000)) / 10.0d);
    record.put("payload", ByteBuffer.wrap(data.consumeBytes(data.consumeInt(0, 16))));

    List<String> tags = new ArrayList<>();
    for (int i = 0, size = data.consumeInt(0, 3); i < size; i++) {
      tags.add(safeString(data.consumeString(16), "tag" + i));
    }
    record.put("tags", tags);

    Map<String, Long> counts = new LinkedHashMap<>();
    for (int i = 0, size = data.consumeInt(0, 3); i < size; i++) {
      String key = safeString(data.consumeString(12), "k" + i);
      counts.put(key, data.consumeLong());
    }
    record.put("counts", counts);

    switch (data.consumeInt(0, 2)) {
      case 0:
        record.put("choice", null);
        break;
      case 1:
        record.put("choice", safeString(data.consumeString(16), "choice"));
        break;
      default:
        record.put("choice", data.consumeLong());
        break;
    }

    GenericRecord inner = new GenericData.Record(PARQUET_SCHEMA.getField("inner").schema());
    inner.put("x", data.consumeInt());
    inner.put("y", data.consumeInt());
    record.put("inner", inner);

    return record;
  }

  @SuppressWarnings("unchecked")
  private static boolean parquetRecordsEqual(GenericRecord expected, GenericRecord actual) {
    for (Schema.Field field : expected.getSchema().getFields()) {
      if (!parquetDatumEquals(expected.get(field.name()), actual.get(field.name()))) {
        return false;
      }
    }
    return true;
  }

  private static boolean parquetDatumEquals(Object left, Object right) {
    if (left == null && right == null) {
      return true;
    }
    if (left == null || right == null) {
      return false;
    }
    if (left instanceof CharSequence && right instanceof CharSequence) {
      return left.toString().equals(right.toString());
    }
    if (left instanceof ByteBuffer && right instanceof ByteBuffer) {
      return left.equals(right);
    }
    if (left instanceof Map && right instanceof Map) {
      Map<?, ?> leftMap = (Map<?, ?>) left;
      Map<?, ?> rightMap = (Map<?, ?>) right;
      if (leftMap.size() != rightMap.size()) {
        return false;
      }
      for (Map.Entry<?, ?> entry : leftMap.entrySet()) {
        String key = entry.getKey().toString();
        Object rightVal = null;
        for (Map.Entry<?, ?> re : rightMap.entrySet()) {
          if (re.getKey().toString().equals(key)) {
            rightVal = re.getValue();
            break;
          }
        }
        if (!parquetDatumEquals(entry.getValue(), rightVal)) {
          return false;
        }
      }
      return true;
    }
    if (left instanceof List && right instanceof List) {
      List<?> leftList = (List<?>) left;
      List<?> rightList = (List<?>) right;
      if (leftList.size() != rightList.size()) {
        return false;
      }
      for (int i = 0; i < leftList.size(); i++) {
        if (!parquetDatumEquals(leftList.get(i), rightList.get(i))) {
          return false;
        }
      }
      return true;
    }
    if (left instanceof GenericRecord && right instanceof GenericRecord) {
      return parquetRecordsEqual((GenericRecord) left, (GenericRecord) right);
    }
    return Objects.equals(left, right);
  }

  // --- Utility helpers ---

  private static String safeString(String value, String fallback) {
    return value.isEmpty() ? fallback : value;
  }

  /**
   * Converts a string to a valid identifier (starts with letter, only
   * alphanumeric characters).
   */
  private static String toIdentifier(String raw, String fallback) {
    if (raw == null || raw.isEmpty()) {
      return fallback;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (i == 0 && Character.isLetter(c)) {
        sb.append(c);
      } else if (i > 0 && Character.isLetterOrDigit(c)) {
        sb.append(c);
      }
    }
    return sb.length() == 0 ? fallback : sb.toString();
  }

  /**
   * Generates fuzzed Avro schema JSON using different structural templates.
   */
  private static String buildAvroSchemaInput(FuzzedDataProvider data) {
    int mode = data.consumeInt(0, 3);
    String name = toIdentifier(data.consumeString(16), "SeedRecord");
    String fieldName = toIdentifier(data.consumeString(16), "field");
    String remaining = data.consumeRemainingAsString();

    switch (mode) {
      case 0:
        return remaining;
      case 1:
        return "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[" + remaining + "]}";
      case 2:
        return "[\"null\"," + remaining + "]";
      default:
        return "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[{\"name\":\"" + fieldName
            + "\",\"type\":" + remaining + "}]}";
    }
  }

  // --- In-memory OutputFile for Parquet ---

  static final class InMemoryOutputFile implements OutputFile {
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new InMemoryPositionOutputStream(baos);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      baos.reset();
      return new InMemoryPositionOutputStream(baos);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }

    byte[] toByteArray() {
      return baos.toByteArray();
    }
  }

  private static final class InMemoryPositionOutputStream extends PositionOutputStream {
    private final ByteArrayOutputStream delegate;
    private long position = 0;

    InMemoryPositionOutputStream(ByteArrayOutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void write(int b) throws IOException {
      delegate.write(b);
      position++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      delegate.write(b, off, len);
      position += len;
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
