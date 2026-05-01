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

import com.code_intelligence.jazzer.junit.FuzzTest;
import java.io.IOException;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/**
 * Fuzz test for {@link ParquetFileReader}.
 *
 * <p>Feeds random byte arrays to the Parquet file reader via {@link InMemoryInputFile}.
 * Exercises magic byte validation, Thrift-encoded footer metadata deserialization,
 * row group and column chunk metadata parsing, page header decoding and data page
 * decompression, and dictionary page handling.
 *
 * <p>This is the highest-value security fuzzing target for Parquet: in production
 * environments, Parquet files may be received from untrusted sources (data lakes,
 * cross-organization data sharing, user uploads), making the parser a critical
 * attack surface.
 */
public class ParquetFileReadFuzzTest {

  /**
   * OSS-Fuzz entry point. Attempts to open and read all row groups from
   * arbitrary bytes as a Parquet file.
   */
  public static void fuzzerTestOneInput(byte[] data) {
    parseParquetFile(data);
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzParquetFileRead(byte[] inputData) {
    parseParquetFile(inputData);
  }

  private static void parseParquetFile(byte[] data) {
    // Parquet files must be at least 12 bytes (4 magic + 4 footer length + 4 magic)
    if (data.length < 12) {
      return;
    }

    try {
      InMemoryInputFile inputFile = new InMemoryInputFile(data);
      try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
        ParquetMetadata footer = reader.getFooter();
        if (footer == null) {
          return;
        }
        // Iterate through all row groups to exercise page reading
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          // Force materialization of column data
          pages.getRowCount();
        }
      }
    } catch (IOException | RuntimeException e) {
      // Expected: malformed data will cause various parse/decode errors.
      // Parquet throws plain RuntimeException, IllegalArgumentException,
      // IllegalStateException, NullPointerException, IndexOutOfBoundsException,
      // and others for corrupt files.
    } catch (OutOfMemoryError e) {
      // Malformed metadata can declare huge row counts or page sizes
    }
  }

}
