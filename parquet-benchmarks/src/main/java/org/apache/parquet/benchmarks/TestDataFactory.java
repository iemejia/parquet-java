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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

/**
 * Utility class for generating test schemas and data for benchmarks.
 */
public final class TestDataFactory {

  /** Default number of rows for file-level benchmarks. */
  public static final int DEFAULT_ROW_COUNT = 100_000;

  /** Default RNG seed used across benchmarks for deterministic data. */
  public static final long DEFAULT_SEED = 42L;

  /** A standard multi-type schema used by file-level benchmarks. */
  public static final MessageType FILE_BENCHMARK_SCHEMA = Types.buildMessage()
      .required(INT32)
      .named("int32_field")
      .required(INT64)
      .named("int64_field")
      .required(FLOAT)
      .named("float_field")
      .required(DOUBLE)
      .named("double_field")
      .required(BOOLEAN)
      .named("boolean_field")
      .required(BINARY)
      .named("binary_field")
      .named("benchmark_record");

  private TestDataFactory() {}

  // ---- File-level (Group) data generation ----

  /**
   * Creates a {@link SimpleGroupFactory} for the standard benchmark schema.
   */
  public static SimpleGroupFactory newGroupFactory() {
    return new SimpleGroupFactory(FILE_BENCHMARK_SCHEMA);
  }

  /**
   * Generates a single row of benchmark data.
   *
   * @param factory the group factory
   * @param index   the row index (used for deterministic data)
   * @param random  the random source
   * @return a populated Group
   */
  public static Group generateRow(SimpleGroupFactory factory, int index, Random random) {
    return factory.newGroup()
        .append("int32_field", index)
        .append("int64_field", (long) index * 100)
        .append("float_field", random.nextFloat())
        .append("double_field", random.nextDouble())
        .append("boolean_field", index % 2 == 0)
        .append("binary_field", "value_" + (index % 1000));
  }

  /**
   * Generates a deterministic set of rows for file-level benchmarks.
   */
  public static Group[] generateRows(SimpleGroupFactory factory, int rowCount, long seed) {
    Group[] rows = new Group[rowCount];
    Random random = new Random(seed);
    for (int i = 0; i < rowCount; i++) {
      rows[i] = generateRow(factory, i, random);
    }
    return rows;
  }

  // ---- Fixed-length byte array data generation ----

  /**
   * Generates fixed-length byte arrays with the specified cardinality.
   *
   * @param count      number of values
   * @param length     byte length of each value
   * @param distinct   number of distinct values (0 means all unique)
   * @param seed       RNG seed
   */
  public static Binary[] generateFixedLenByteArrays(int count, int length, int distinct, long seed) {
    Random random = new Random(seed);
    if (distinct > 0) {
      Binary[] palette = new Binary[distinct];
      for (int i = 0; i < distinct; i++) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        palette[i] = Binary.fromConstantByteArray(bytes);
      }
      Binary[] data = new Binary[count];
      for (int i = 0; i < count; i++) {
        data[i] = palette[random.nextInt(distinct)];
      }
      return data;
    } else {
      Binary[] data = new Binary[count];
      for (int i = 0; i < count; i++) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        data[i] = Binary.fromConstantByteArray(bytes);
      }
      return data;
    }
  }

  // ---- Binary data generation ----

  /**
   * Generates binary strings of the given length with the specified cardinality.
   *
   * @param count         number of values
   * @param stringLength  length of each string
   * @param distinct      number of distinct values (0 means all unique)
   * @param seed          RNG seed
   */
  public static Binary[] generateBinaryData(int count, int stringLength, int distinct, long seed) {
    Random random = new Random(seed);
    Binary[] data = new Binary[count];
    if (distinct > 0) {
      Binary[] dictionary = new Binary[distinct];
      for (int i = 0; i < distinct; i++) {
        dictionary[i] = Binary.fromConstantByteArray(
            randomString(stringLength, random).getBytes(StandardCharsets.UTF_8));
      }
      for (int i = 0; i < count; i++) {
        data[i] = dictionary[random.nextInt(distinct)];
      }
    } else {
      for (int i = 0; i < count; i++) {
        data[i] = Binary.fromConstantByteArray(
            randomString(stringLength, random).getBytes(StandardCharsets.UTF_8));
      }
    }
    return data;
  }

  private static String randomString(int length, Random random) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append((char) ('a' + random.nextInt(26)));
    }
    return sb.toString();
  }
}
