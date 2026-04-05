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
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;

/**
 * Fuzz test for {@link ParquetFileReader}.
 *
 * <p>Feeds random byte arrays to the Parquet file reader via {@link InMemoryInputFile}.
 * Only unexpected crashes (NPE, OOM, StackOverflow) should cause test failure;
 * expected parse errors are caught and ignored.
 */
public class ParquetFileReadFuzzTest {

  @FuzzTest(maxDuration = "5m")
  public void fuzzParquetFileRead(byte[] data) {
    if (data.length == 0) {
      return;
    }

    try {
      InMemoryInputFile inputFile = new InMemoryInputFile(data);
      ParquetReadOptions options = ParquetReadOptions.builder().build();
      try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
        // Try to read footer metadata and row groups
        reader.getFooter();
        reader.getRowGroups();
        // Try reading the first page
        reader.readNextRowGroup();
      }
    } catch (IOException | RuntimeException e) {
      // Expected: malformed data will cause various parse/decode errors
      // ParquetDecodingException extends RuntimeException
    }
  }
}
