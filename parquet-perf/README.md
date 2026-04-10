<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# parquet-perf

JMH benchmarks for Apache Parquet Java, measuring throughput, latency, and memory
consumption of the most critical read/write and encoding operations.

## Module structure

```
parquet-perf/
  src/main/java/org/apache/parquet/perf/
    util/
      BlackHoleOutputFile.java           # No-op OutputFile (isolates CPU from I/O)
      TestDataFactory.java               # Schema + data generation utilities
    file/
      FileWriteBenchmark.java            # File-level write (codec, V1/V2, dict)
      FileReadBenchmark.java             # File-level read  (codec, V1/V2)
    encoding/
      IntEncodingBenchmark.java          # INT32: PLAIN, DELTA, BSS, DICTIONARY
      BinaryEncodingBenchmark.java       # BINARY: PLAIN, DELTA_BA, DELTA_LEN, DICT
    concurrent/
      ConcurrentReadWriteBenchmark.java  # @Threads(4) multi-threaded read/write
  src/test/java/org/apache/parquet/perf/
    correctness/
      ConcurrentCorrectnessTest.java     # JUnit: verify data integrity under concurrency
```

## Building

```bash
# From the repository root
./mvnw -pl parquet-perf package -DskipTests
```

This creates an uber-jar at `parquet-perf/target/parquet-perf.jar`.

## Running benchmarks

```bash
# All benchmarks
./run.sh

# Specific benchmark by regex
./run.sh FileWrite
./run.sh IntEncoding

# With GC/memory profiling
./run.sh FileWrite -prof gc

# With JSON output (visualize at https://jmh.morethan.io)
./run.sh -rf json

# Override thread count for concurrent benchmarks
./run.sh ConcurrentReadWrite -t 8

# Filter by parameter
./run.sh IntEncoding -p dataPattern=SEQUENTIAL
```

## Benchmark parameters

### File-level write (`FileWriteBenchmark`)

| Parameter      | Values                                        |
|----------------|-----------------------------------------------|
| codec          | UNCOMPRESSED, SNAPPY, ZSTD, GZIP             |
| writerVersion  | PARQUET_1_0, PARQUET_2_0                      |
| dictionary     | true, false                                   |

### File-level read (`FileReadBenchmark`)

| Parameter      | Values                                        |
|----------------|-----------------------------------------------|
| codec          | UNCOMPRESSED, SNAPPY, ZSTD, GZIP             |
| writerVersion  | PARQUET_1_0, PARQUET_2_0                      |

### INT32 encoding (`IntEncodingBenchmark`)

| Parameter   | Values                                            |
|-------------|---------------------------------------------------|
| dataPattern | SEQUENTIAL, RANDOM, LOW_CARDINALITY, HIGH_CARDINALITY |

Encodings benchmarked: PLAIN, DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT, DICTIONARY (encode only for DICTIONARY).

### Binary encoding (`BinaryEncodingBenchmark`)

| Parameter    | Values             |
|--------------|--------------------|
| stringLength | 10, 100, 1000     |
| cardinality  | LOW, HIGH          |

Encodings benchmarked: PLAIN, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, DICTIONARY (encode only for DICTIONARY).

## Memory profiling

Use JMH's built-in GC profiler to measure heap allocation rates:

```bash
./run.sh -prof gc
```

Key metrics reported:
- `gc.alloc.rate` — allocation rate (MB/s)
- `gc.alloc.rate.norm` — bytes allocated per operation
- `gc.count` / `gc.time` — GC pauses during measurement

## Correctness tests

The module includes a JUnit test that verifies data integrity under concurrency:

```bash
./mvnw -pl parquet-perf test
```

This runs 8 concurrent threads, each writing and reading back a separate Parquet file,
verifying every row matches expected values.
