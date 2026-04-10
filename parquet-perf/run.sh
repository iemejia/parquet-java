#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# ============================================================================
# parquet-perf benchmark runner
# ============================================================================
#
# Usage:
#   ./run.sh                          # run all benchmarks
#   ./run.sh <regex>                  # run benchmarks matching regex
#   ./run.sh FileWrite                # run only FileWriteBenchmark
#   ./run.sh IntEncoding -prof gc     # run IntEncodingBenchmark with GC profiling
#
# Useful JMH flags:
#   -prof gc              Heap allocation profiling (built-in)
#   -prof stack           Stack profiling
#   -t <n>                Override thread count for concurrent benchmarks
#   -f <n>                Override fork count
#   -wi <n>               Override warmup iterations
#   -i <n>                Override measurement iterations
#   -rf json              Output results in JSON (for https://jmh.morethan.io)
#   -lprof                List available profilers
#
# Examples:
#   ./run.sh FileWrite -prof gc -rf json
#   ./run.sh ".*Concurrent.*" -t 8
#   ./run.sh IntEncoding -p dataPattern=SEQUENTIAL
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${SCRIPT_DIR}/target/parquet-perf.jar"

if [ ! -f "$JAR" ]; then
  echo "Uber-jar not found at $JAR"
  echo "Building parquet-perf module..."
  (cd "${SCRIPT_DIR}/.." && ./mvnw -pl parquet-perf package -DskipTests -q)
fi

echo "Running benchmarks..."
java -jar "$JAR" "$@"
