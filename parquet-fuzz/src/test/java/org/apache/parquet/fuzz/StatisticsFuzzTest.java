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
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

/**
 * Fuzz test for {@link Statistics}.
 *
 * <p>Creates statistics for all primitive types, updates them with fuzzed values,
 * and exercises merge operations.
 */
public class StatisticsFuzzTest {

  private static final PrimitiveType INT32_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "fuzz_int32");
  private static final PrimitiveType INT64_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "fuzz_int64");
  private static final PrimitiveType FLOAT_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "fuzz_float");
  private static final PrimitiveType DOUBLE_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "fuzz_double");
  private static final PrimitiveType BOOLEAN_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, "fuzz_boolean");
  private static final PrimitiveType BINARY_TYPE =
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "fuzz_binary");

  private static final PrimitiveType[] TYPES = {
    INT32_TYPE, INT64_TYPE, FLOAT_TYPE, DOUBLE_TYPE, BOOLEAN_TYPE, BINARY_TYPE
  };

  /** OSS-Fuzz entry point. */
  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    new StatisticsFuzzTest().fuzzStatisticsUpdateAndMerge(data);
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzStatisticsUpdateAndMerge(FuzzedDataProvider data) {
    PrimitiveType type = TYPES[data.consumeInt(0, TYPES.length - 1)];
    int updateCount = data.consumeInt(0, 200);
    int nullCount = data.consumeInt(0, 50);

    try {
      Statistics<?> stats = Statistics.createStats(type);

      // Add nulls
      for (int i = 0; i < nullCount; i++) {
        stats.incrementNumNulls();
      }

      // Update with values
      for (int i = 0; i < updateCount; i++) {
        updateStatsForType(stats, type.getPrimitiveTypeName(), data);
      }

      // Verify min/max contract: if we have values, min <= max
      if (stats.hasNonNullValue()) {
        Comparable min = stats.genericGetMin();
        Comparable max = stats.genericGetMax();
        if (min != null && max != null) {
          @SuppressWarnings("unchecked")
          int cmp = min.compareTo(max);
          // For NaN-aware float/double stats, the contract may differ,
          // so we only assert for non-NaN types
          if (type.getPrimitiveTypeName() != PrimitiveTypeName.FLOAT
              && type.getPrimitiveTypeName() != PrimitiveTypeName.DOUBLE) {
            if (cmp > 0) {
              throw new AssertionError("Statistics min > max: " + min + " > " + max);
            }
          }
        }
      }

      // Test merge: create a second stats and merge
      Statistics<?> stats2 = Statistics.createStats(type);
      int updateCount2 = data.consumeInt(0, 50);
      for (int i = 0; i < updateCount2; i++) {
        updateStatsForType(stats2, type.getPrimitiveTypeName(), data);
      }
      stats.mergeStatistics(stats2);

      // Verify null count
      if (stats.isNumNullsSet()) {
        long numNulls = stats.getNumNulls();
        if (numNulls < 0) {
          throw new AssertionError("Negative null count: " + numNulls);
        }
      }

    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Expected
    }
  }

  private void updateStatsForType(Statistics<?> stats, PrimitiveTypeName typeName, FuzzedDataProvider data) {
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
      case BOOLEAN:
        stats.updateStats(data.consumeBoolean());
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
