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
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

/**
 * Fuzz test for {@link ColumnIndexBuilder}.
 *
 * <p>Builds column indexes with fuzzed page statistics and verifies boundary order invariants.
 */
public class ColumnIndexFuzzTest {

  private static final PrimitiveType INT32_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "fuzz_col");
  private static final PrimitiveType INT64_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "fuzz_col");
  private static final PrimitiveType FLOAT_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "fuzz_col");
  private static final PrimitiveType DOUBLE_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "fuzz_col");
  private static final PrimitiveType BINARY_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "fuzz_col");

  private static final PrimitiveType[] TYPES = {INT32_TYPE, INT64_TYPE, FLOAT_TYPE, DOUBLE_TYPE, BINARY_TYPE};

  @FuzzTest(maxDuration = "5m")
  public void fuzzColumnIndex(FuzzedDataProvider data) {
    PrimitiveType type = TYPES[data.consumeInt(0, TYPES.length - 1)];
    int pageCount = data.consumeInt(0, 100);
    int truncateLength = data.consumeInt(1, 128);

    try {
      ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, truncateLength);

      for (int p = 0; p < pageCount; p++) {
        boolean isNullPage = data.consumeBoolean();

        Statistics<?> pageStats = Statistics.createStats(type);

        if (isNullPage) {
          // Null-only page: just increment null count
          int nullCount = data.consumeInt(1, 100);
          for (int n = 0; n < nullCount; n++) {
            pageStats.incrementNumNulls();
          }
        } else {
          // Page with values
          int valueCount = data.consumeInt(1, 50);
          int nullCount = data.consumeInt(0, 10);
          for (int n = 0; n < nullCount; n++) {
            pageStats.incrementNumNulls();
          }
          for (int v = 0; v < valueCount; v++) {
            updateStats(pageStats, type.getPrimitiveTypeName(), data);
          }
        }

        builder.add(pageStats);
      }

      ColumnIndex columnIndex = builder.build();

      if (columnIndex != null && pageCount > 0) {
        // Verify structural invariants
        BoundaryOrder order = columnIndex.getBoundaryOrder();
        if (order == null) {
          throw new AssertionError("BoundaryOrder should not be null");
        }

        int nullPagesSize = columnIndex.getNullPages().size();
        if (nullPagesSize != pageCount) {
          throw new AssertionError("NullPages size mismatch: expected " + pageCount + ", got " + nullPagesSize);
        }

        int minValuesSize = columnIndex.getMinValues().size();
        int maxValuesSize = columnIndex.getMaxValues().size();
        if (minValuesSize != maxValuesSize) {
          throw new AssertionError(
              "MinValues/MaxValues size mismatch: " + minValuesSize + " vs " + maxValuesSize);
        }
      }

    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Expected for certain fuzz inputs
    }
  }

  private void updateStats(Statistics<?> stats, PrimitiveTypeName typeName, FuzzedDataProvider data) {
    switch (typeName) {
      case INT32:
        stats.updateStats(data.consumeInt());
        break;
      case INT64:
        stats.updateStats(data.consumeLong());
        break;
      case FLOAT:
        stats.updateStats(data.consumeFloat());
        break;
      case DOUBLE:
        stats.updateStats(data.consumeDouble());
        break;
      case BINARY:
        byte[] bytes = data.consumeBytes(data.consumeInt(0, 64));
        stats.updateStats(Binary.fromConstantByteArray(bytes));
        break;
      default:
        break;
    }
  }
}
