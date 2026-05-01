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
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.io.api.Binary;

/**
 * Fuzz test for {@link BlockSplitBloomFilter}.
 *
 * <p>Tests insert/find roundtrip consistency and construction from raw byte arrays.
 */
public class BloomFilterFuzzTest {

  /** OSS-Fuzz entry point. Multiplexes between roundtrip and raw-bytes modes. */
  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    int mode = data.consumeInt(0, 1);
    BloomFilterFuzzTest instance = new BloomFilterFuzzTest();
    switch (mode) {
      case 0:
        instance.fuzzBloomFilterRoundtrip(data);
        break;
      default:
        instance.fuzzBloomFilterFromBytes(data);
        break;
    }
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzBloomFilterRoundtrip(FuzzedDataProvider data) {
    // numBytes must be in [LOWER_BOUND_BYTES, UPPER_BOUND_BYTES] and will be rounded to power of 2
    int numBytes = data.consumeInt(
        BlockSplitBloomFilter.LOWER_BOUND_BYTES, Math.min(4096, BlockSplitBloomFilter.UPPER_BOUND_BYTES));
    int insertCount = data.consumeInt(0, 200);

    try {
      BlockSplitBloomFilter filter = new BlockSplitBloomFilter(numBytes);

      for (int i = 0; i < insertCount; i++) {
        int valueType = data.consumeInt(0, 4);
        long hash;
        switch (valueType) {
          case 0:
            hash = filter.hash(data.consumeInt());
            break;
          case 1:
            hash = filter.hash(data.consumeLong());
            break;
          case 2:
            hash = filter.hash(data.consumeFloat());
            break;
          case 3:
            hash = filter.hash(data.consumeDouble());
            break;
          default:
            byte[] binaryBytes = data.consumeBytes(data.consumeInt(0, 64));
            hash = filter.hash(Binary.fromConstantByteArray(binaryBytes));
            break;
        }
        filter.insertHash(hash);

        // After insertion, findHash must return true (no false negatives)
        if (!filter.findHash(hash)) {
          throw new AssertionError("Bloom filter false negative: hash " + hash + " not found after insertion");
        }
      }
    } catch (IllegalArgumentException e) {
      // Expected for invalid filter sizes
    }
  }

  @FuzzTest(maxDuration = "5m")
  public void fuzzBloomFilterFromBytes(FuzzedDataProvider data) {
    byte[] bitset = data.consumeRemainingAsBytes();
    if (bitset.length == 0) {
      return;
    }

    try {
      BlockSplitBloomFilter filter = new BlockSplitBloomFilter(bitset);
      // Exercise the filter with some lookups
      filter.findHash(filter.hash(42));
      filter.findHash(filter.hash(0L));
      filter.findHash(filter.hash(3.14f));
      filter.findHash(filter.hash(2.718));
      filter.findHash(filter.hash(Binary.fromConstantByteArray(new byte[] {1, 2, 3})));
    } catch (RuntimeException e) {
      // Expected for invalid bitset data (includes IllegalArgumentException)
    }
  }
}
