# Parquet-Java Performance Improvement Analysis

## Overview

This document describes the performance analysis and optimizations applied to the
parquet-java (formerly parquet-mr) encoding hot paths, driven by profiling the existing
JMH benchmarks in the `parquet-benchmarks` module.

Three optimizations were implemented in the `parquet-column` module, targeting the
encoding and decoding paths exercised by `IntEncodingBenchmark`, `BinaryEncodingBenchmark`,
and `FileReadBenchmark`. All changes were validated with JMH benchmarks and the full
`parquet-column` test suite (573 tests, 0 failures).

---

## Methodology

### Benchmark Infrastructure

The project contains 12 JMH benchmark classes in `parquet-benchmarks/` covering:

| Benchmark | What it measures | Mode |
|-----------|-----------------|------|
| `IntEncodingBenchmark` | INT32 encode/decode (PLAIN, DELTA, BSS, DICT) | Throughput |
| `BinaryEncodingBenchmark` | BINARY encode/decode (PLAIN, DELTA_BA, DELTA_LBA, DICT) | Throughput |
| `FileWriteBenchmark` | Full write pipeline to BlackHoleOutputFile | SingleShot + Avg |
| `FileReadBenchmark` | Full read pipeline from temp file | SingleShot + Avg |
| `ReadBenchmarks` / `WriteBenchmarks` | Full I/O pipeline with compression | SingleShot + Avg |
| `ConcurrentReadWriteBenchmark` | Multi-threaded (4T) read/write | SingleShot + Avg |
| `FilteringBenchmarks` | Column index filtering | SingleShot |
| `PageChecksumRead/WriteBenchmarks` | CRC checksum overhead | SingleShot |
| `NestedNullWritingBenchmarks` | Nested null value writing (PARQUET-343) | SingleShot |

### Analysis Approach

1. Read all benchmark source code to identify which encoding hot paths are exercised
2. Read the encoder/decoder implementations in `parquet-column` to identify bottlenecks
3. Cross-reference with allocation patterns, cache access patterns, and loop structures
4. Implement targeted fixes and validate with before/after JMH runs
5. Run the full test suite to confirm zero regressions

### Benchmark Configuration

All benchmarks run with: `-wi 3 -i 5 -f 1` (3 warmup iterations, 5 measurement iterations, 1 fork).
The encoding micro-benchmarks process 100,000 values per invocation.

---

## Improvement 1: ByteStreamSplitValuesWriter — Eliminate Per-Value Allocation

### File Changed

`parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesWriter.java`

### Problem

Every call to `writeInteger()`, `writeLong()`, `writeFloat()`, or `writeDouble()` invoked
`BytesUtils.intToBytes(v)` or `BytesUtils.longToBytes(v)`, which allocates a **new `byte[4]`
or `byte[8]`** on every call. The array is immediately passed to `scatterBytes()` which reads
each byte and discards the array.

For the benchmark configuration of 100,000 values per invocation, this created 100,000
short-lived byte arrays per invocation — significant GC pressure on a throughput benchmark.

### Root Cause

```java
// Before (allocates new byte[4] per call)
public void writeInteger(int v) {
    super.scatterBytes(BytesUtils.intToBytes(v));
}
```

`BytesUtils.intToBytes()` implementation:
```java
public static byte[] intToBytes(int value) {
    byte[] bytes = new byte[4];  // <-- allocation
    bytes[0] = (byte) (value & 0xFF);
    bytes[1] = (byte) ((value >>> 8) & 0xFF);
    bytes[2] = (byte) ((value >>> 16) & 0xFF);
    bytes[3] = (byte) ((value >>> 24) & 0xFF);
    return bytes;
}
```

### Fix

Added two protected helper methods `scatterInt(int)` and `scatterLong(long)` that inline
the byte extraction using bit shifts, writing directly to each stream without any
intermediate array:

```java
protected void scatterInt(int v) {
    this.byteStreams[0].write(v & 0xFF);
    this.byteStreams[1].write((v >>> 8) & 0xFF);
    this.byteStreams[2].write((v >>> 16) & 0xFF);
    this.byteStreams[3].write((v >>> 24) & 0xFF);
}
```

Updated all four subclasses (`Float`, `Double`, `Integer`, `Long`) to use these methods.
The `FixedLenByteArray` subclass continues to use `scatterBytes()` since it handles
variable-length data.

### Benchmark Results

| Benchmark | Before | After | Change |
|-----------|--------|-------|--------|
| `IntEncodingBenchmark.encodeByteStreamSplit` (RANDOM) | 19,049,928 ops/s | 22,702,527 ops/s | **+19.2%** |

### Tests

All 28 ByteStreamSplit tests pass:
- `ByteStreamSplitValuesWriterTest` (7 tests)
- `ByteStreamSplitValuesReaderTest` (16 tests)
- `ByteStreamSplitValuesEndToEndTest` (5 tests)

---

## Improvement 2: RunLengthBitPackingHybridDecoder — Reuse Packed Run Buffers

### File Changed

`parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`

### Problem

The `readNext()` method allocated **two new arrays on every packed run**:

```java
currentBuffer = new int[currentCount]; // TODO: reuse a buffer
byte[] bytes = new byte[numGroups * bitWidth];
```

The `TODO: reuse a buffer` comment was already in the codebase, acknowledging this as a
known inefficiency. The RLE/bit-packing hybrid decoder is used throughout Parquet for
decoding definition levels, repetition levels, and dictionary indices. During file reads
with many columns, this creates substantial allocation pressure.

### Fix

Promoted both buffers to instance fields. They are only reallocated when a larger size
is needed, otherwise the existing buffer is reused:

```java
private int[] packedValuesBuffer = new int[0];
private byte[] packedBytesBuffer = new byte[0];

// In readNext(), PACKED case:
if (packedValuesBuffer.length < currentCount) {
    packedValuesBuffer = new int[currentCount];
}
currentBuffer = packedValuesBuffer;
```

A `currentBufferLength` field was added to track the logical size of the buffer
(required because `readInt()` uses length-based indexing: `currentBuffer[currentBufferLength - 1 - currentCount]`).

### Benchmark Results

The RLE decoder is not directly exercised by the encoding micro-benchmarks (they test
value-level encoding, not the RLE layer). It IS exercised during file-level reads
(for def/rep level decoding):

| Benchmark | Before | After | Change |
|-----------|--------|-------|--------|
| `FileReadBenchmark.readFile` (UNCOMPRESSED, V2, avgt) | 89.1 ms/op | 87.1 ms/op | **~2.2%** |
| `FileReadBenchmark.readFile` (UNCOMPRESSED, V2, ss) | 113.3 ms/op | 111.8 ms/op | **~1.3%** |

The improvement is modest in file-level benchmarks because RLE decoding is only one
component of the full read pipeline (alongside decompression, page deserialization,
and record materialization). The allocation reduction is more significant under high
concurrency and with many-column schemas.

### Tests

All 52 related tests pass:
- `TestRunLengthBitPackingHybridEncoder` (9 tests)
- `RunLengthBitPackingHybridIntegrationTest` (1 test — all bit widths 0-32)
- `DeltaBinaryPackingValuesWriterForIntegerTest` (16 tests, including 100K random rounds)
- `DeltaBinaryPackingValuesWriterForLongTest` (16 tests, including 100K random rounds)
- `TestDeltaByteArray` (6 tests)
- `TestDeltaLengthByteArray` (4 tests)

---

## Improvement 3: ByteStreamSplitValuesReader — Cache-Friendly Gather Loop

### File Changed

`parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReader.java`

### Problem

The `decodeData()` method reconstructs interleaved byte streams into contiguous elements.
The original loop order iterated per-value (outer) and per-stream (inner):

```java
for (int srcValueIndex = 0; srcValueIndex < valuesCount; ++srcValueIndex) {
    for (int stream = 0; stream < elementSizeInBytes; ++stream) {
        decoded[destByteIndex++] = encoded.get(srcValueIndex + stream * valuesCount);
    }
}
```

For integer decoding (`elementSizeInBytes = 4`) with 100,000 values:
- **Write pattern**: Sequential (good)
- **Read pattern**: Jumps by `valuesCount` (100,000 bytes) in the inner loop — accessing
  4 different cache lines ~100KB apart per value. This is **extremely cache-unfriendly**
  for the CPU prefetcher.

### Fix

Restructured the loop to iterate per-stream (outer) with sequential reads, writing
at a small stride:

```java
for (int stream = 0; stream < elementSizeInBytes; ++stream) {
    int srcOffset = stream * valuesCount;
    for (int i = 0; i < valuesCount; ++i) {
        decoded[i * elementSizeInBytes + stream] = encoded.get(srcOffset + i);
    }
}
```

- **Read pattern**: Sequential within each stream (good — CPU prefetcher works optimally)
- **Write pattern**: Stride of `elementSizeInBytes` (4 or 8 bytes) — the entire output
  array (400KB for 100K ints) fits in L2 cache, so the small write stride doesn't cause
  significant cache evictions.

### Benchmark Results

| Benchmark | Before | After | Change |
|-----------|--------|-------|--------|
| `IntEncodingBenchmark.decodeByteStreamSplit` (RANDOM) | 46,140,045 ops/s | 86,357,036 ops/s | **+87.2%** |

This is the largest improvement — nearly doubling the decode throughput — because the
original code had a severely cache-hostile access pattern that dominated execution time.

### Tests

All 28 ByteStreamSplit tests pass (same set as Improvement 1).

---

## Investigations That Were Ruled Out

During the analysis, several potential optimizations were investigated and found to be
either already addressed or not safe to implement:

| Investigation | Finding | Action |
|---|---|---|
| **BytePacker caching in delta writer/reader** | Despite `newBytePacker()` naming, the factory already returns **cached singletons** from a static array (generated by `ByteBasedBitPackingGenerator`). The TODO comments in `DeltaBinaryPackingValuesWriterForLong:120` are misleading. | No change needed |
| **DeltaByteArrayReader buffer reuse** | Each `readBytes()` allocates `new byte[length]` when prefix > 0. However, the returned `Binary` **shares** the backing array with the `previous` field. Callers may hold references to returned Binaries, making buffer reuse **unsafe** without changing the API contract. | No change — correctness risk |
| **CapacityByteArrayOutputStream.write(int) Math.addExact** | Single-byte writes use `Math.addExact(bytesUsed, 1)` for overflow safety. While marginally slower than plain addition, removing it would sacrifice a safety guarantee for negligible gain. | No change — safety feature |
| **LittleEndianDataInputStream/OutputStream overhead** | Plain encoding uses these wrappers. They add virtual dispatch but the JIT inlines them effectively. No measurable overhead in benchmarks. | No change |

---

## Summary of Results

| # | Optimization | Module | Benchmark | Improvement |
|---|---|---|---|---|
| 1 | BSS writer: inline scatter | parquet-column | encodeByteStreamSplit | **+19.2%** |
| 2 | RLE decoder: buffer reuse | parquet-column | FileReadBenchmark | **~2.2%** |
| 3 | BSS reader: cache-friendly loop | parquet-column | decodeByteStreamSplit | **+87.2%** |

### Test Results

- **parquet-column**: 573 tests, 0 failures, 0 errors
- **parquet-hadoop**: Pre-existing failures only (crypto/statistics tests unrelated to encoding); all encoding-related tests pass
- **parquet-avro**: Pre-existing failures only (TestByteStreamSplitE2E fails on clean master)

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesWriter.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReader.java`
3. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`

### Commit

```
136c751f1 Optimize encoding hot paths: ByteStreamSplit writer/reader and RLE decoder
```

---

## Round 2: Plain Encoding and Delta Byte Array Optimizations

### Improvement 4: LittleEndianDataOutputStream — Bulk writeInt/writeShort

#### File Changed

`parquet-common/src/main/java/org/apache/parquet/bytes/LittleEndianDataOutputStream.java`

#### Problem

`writeInt()` made **4 separate `out.write(int)` calls**, each going through
`CapacityByteArrayOutputStream`'s `hasRemaining()` check, `currentSlab.put()`, and
`Math.addExact(bytesUsed, 1)`. Similarly `writeShort()` made 2 separate calls.

Meanwhile, `writeLong()` in the **same file** already used the correct pattern: fill a
pre-allocated `writeBuffer[]` and call `out.write(writeBuffer, 0, 8)` — a single bulk
operation that copies all bytes at once via `ByteBuffer.put(byte[], off, len)` with only
one `hasRemaining()` check and one size update.

The code even had a TODO comment in `writeInt()`:
```java
// TODO: see note in LittleEndianDataInputStream: maybe faster
// to use Integer.reverseBytes() and then writeInt, or a ByteBuffer approach
```

#### Fix

Moved the `writeBuffer` field declaration to the top of the class (before `writeShort`),
and changed `writeInt()` and `writeShort()` to use the same bulk-write pattern as `writeLong()`:

```java
public final void writeInt(int v) throws IOException {
    writeBuffer[0] = (byte) (v >>> 0);
    writeBuffer[1] = (byte) (v >>> 8);
    writeBuffer[2] = (byte) (v >>> 16);
    writeBuffer[3] = (byte) (v >>> 24);
    out.write(writeBuffer, 0, 4);
}
```

#### Benchmark Results

| Benchmark | Pattern | Before | After | Change |
|-----------|---------|--------|-------|--------|
| `IntEncodingBenchmark.encodePlain` | SEQUENTIAL | 20,944,408 | 28,403,869 | **+35.6%** |
| `IntEncodingBenchmark.encodePlain` | RANDOM | 20,787,787 | 29,005,923 | **+39.5%** |
| `IntEncodingBenchmark.encodePlain` | LOW_CARDINALITY | 21,051,294 | 28,728,823 | **+36.5%** |
| `IntEncodingBenchmark.encodePlain` | HIGH_CARDINALITY | 20,991,125 | 26,599,662 | **+26.7%** |
| `IntEncodingBenchmark.decodePlain` | (all) | ~90.8M | ~93.1M | No regression |

Average across all patterns: **~20.9M → ~28.2M ops/s (+35%)**

#### Why This Works

`CapacityByteArrayOutputStream.write(byte[], off, len)` does a single `ByteBuffer.put(byte[], off, len)`
which is a single `System.arraycopy` under the hood — versus 4 separate `write(int)` calls, each
performing a `ByteBuffer.put(byte)` + bounds check + `Math.addExact`.

---

### Improvement 5: DeltaByteArrayWriter — Avoid Unnecessary Array Copy

#### File Changed

`parquet-column/src/main/java/org/apache/parquet/column/values/deltastrings/DeltaByteArrayWriter.java`

#### Problem

`writeBytes(Binary v)` called `v.getBytes()` to get a `byte[]` for prefix comparison.
`Binary.getBytes()` **always creates a defensive copy** of the backing data — confirmed
across all four `Binary` inner implementations:

- `ByteArrayBackedBinary.getBytes()` → `Arrays.copyOfRange()`
- `ByteArraySliceBackedBinary.getBytes()` → `Arrays.copyOfRange()`
- `ByteBufferBackedBinary.getBytes()` → `ByteBuffer.get(new byte[])`
- `FromStringBinary.getBytes()` → `Arrays.copyOfRange()`

This is unnecessary when the caller (benchmark or real application) passes constant
(non-reused) `Binary` instances — which is the common case.

#### Fix

Replaced `v.getBytes()` with `v.copy().getBytesUnsafe()`:

- `Binary.copy()` is a **no-op** for constant (non-reused) Binaries (returns `this`)
  and creates a defensive copy only for reused Binaries
- `Binary.getBytesUnsafe()` returns the raw backing `byte[]` directly for
  `ByteArrayBackedBinary` — zero-copy

So for the common case of constant `ByteArrayBackedBinary`, this eliminates
the `Arrays.copyOfRange()` entirely. For the reuse case, `copy()` creates the
defensive copy and `getBytesUnsafe()` returns it — same safety guarantees as before.

```java
// Before: always copies
byte[] vb = v.getBytes();

// After: zero-copy for constant ByteArrayBackedBinary
byte[] vb = v.copy().getBytesUnsafe();
```

#### Benchmark Results

| Benchmark | Cardinality | StringLength | Before | After | Change |
|-----------|------------|-------------|--------|-------|--------|
| `encodeDeltaByteArray` | LOW | 10 | 11,488,761 | 13,968,691 | **+21.6%** |
| `encodeDeltaByteArray` | LOW | 100 | 4,677,921 | 5,026,758 | **+7.5%** |
| `encodeDeltaByteArray` | LOW | 1000 | 628,853 | 644,769 | **+2.5%** |
| `encodeDeltaByteArray` | HIGH | 10 | 10,164,603 | 11,956,074 | **+17.6%** |
| `encodeDeltaByteArray` | HIGH | 100 | 4,015,271 | 4,416,656 | **+10.0%** |
| `encodeDeltaByteArray` | HIGH | 1000 | 586,047 | 706,763 | **+20.6%** |

The improvement is largest for short strings where the copy avoidance savings are
proportionally significant relative to the other work per value.

---

### Investigation: LittleEndianDataInputStream Bulk Read (REVERTED)

The symmetric optimization was attempted on the read side: changing `readInt()` from
4 separate `in.read()` calls to `readFully(readBuffer, 0, 4)` + byte assembly, matching
the existing `readLong()` pattern.

**Result: -64% regression** (90.8M → 32.5M ops/s on `decodePlain`).

**Root cause**: For `ByteBufferInputStream`, individual `read()` calls are highly
optimized — each is just an inlined `ByteBuffer.get() & 0xFF` with no method dispatch
overhead at the JIT level. The bulk `readFully()` path adds:
1. A `while (n < len)` loop with branch prediction overhead
2. `in.read(byte[], int, int)` → `ByteBuffer.get(byte[], off, len)` → `System.arraycopy`
3. Re-assembly of the int from `readBuffer[0..3]` with masking

For only 4 bytes, these overheads dominate. The `readLong()` method was already using
`readFully` in the original code — this was acceptable for 8 bytes but the pattern
does not scale down to 4 bytes.

**Action**: Fully reverted. The read-side code was left unchanged.

---

## Round 3: Delta Binary Packing Read/Write Optimizations

### Improvement 6: DeltaBinaryPackingValuesReader — Slice One Buffer Per Mini Block

#### File Changed

`parquet-column/src/main/java/org/apache/parquet/column/values/delta/DeltaBinaryPackingValuesReader.java`

#### Problem

`DeltaBinaryPackingValuesReader.unpackMiniBlock()` unpacked a 32-value mini block by
calling `unpack8Values()` four times. Each `unpack8Values()` call did its own:

```java
ByteBuffer buffer = in.slice(packer.getBitWidth());
packer.unpack8Values(buffer, buffer.position(), valuesBuffer, valuesBuffered);
```

So the reader created a new `ByteBuffer` view for every 8 decoded values, even though the
entire mini block is a single contiguous region and the packer can read from any offset
within the same buffer.

This extra view creation sits directly in the hot loop exercised by
`IntEncodingBenchmark.decodeDelta`.

#### Fix

Slice the whole mini block once, then reuse that `ByteBuffer` while advancing an integer
byte offset for each 8-value unpack:

```java
int bitWidth = packer.getBitWidth();
int valueCount = config.miniBlockSizeInValues;
ByteBuffer buffer = in.slice((valueCount / 8) * bitWidth);
int bufferPosition = buffer.position();
for (int j = 0, byteOffset = 0; j < valueCount; j += 8, byteOffset += bitWidth) {
    unpack8Values(packer, buffer, bufferPosition + byteOffset);
}
```

This keeps the existing zero-copy decode behavior while removing repeated `ByteBuffer`
slice/view creation from the inner loop.

#### Benchmark Results

| Benchmark | Pattern | Before | After | Change |
|-----------|---------|--------|-------|--------|
| `IntEncodingBenchmark.decodeDelta` | SEQUENTIAL | 134,704,344 | 153,029,731 | **+13.6%** |
| `IntEncodingBenchmark.decodeDelta` | RANDOM | 62,612,403 | 72,668,010 | **+16.1%** |
| `IntEncodingBenchmark.decodeDelta` | LOW_CARDINALITY | 90,060,758 | 101,872,030 | **+13.1%** |
| `IntEncodingBenchmark.decodeDelta` | HIGH_CARDINALITY | 132,037,344 | 150,930,552 | **+14.3%** |

Average across all patterns: **~104.9M → ~119.6M ops/s (+14%)**

---

### Improvement 7: Delta Binary Packing Writers — Use `pack32Values`

#### Files Changed

`parquet-column/src/main/java/org/apache/parquet/column/values/delta/DeltaBinaryPackingValuesWriterForInteger.java`

`parquet-column/src/main/java/org/apache/parquet/column/values/delta/DeltaBinaryPackingValuesWriterForLong.java`

#### Problem

Each delta writer flushed a 32-value mini block using four separate `pack8Values()` calls:

```java
for (int j = miniBlockStart; j < (i + 1) * config.miniBlockSizeInValues; j += 8) {
    packer.pack8Values(deltaBlockBuffer, j, miniBlockByteBuffer, blockOffset);
    blockOffset += currentBitWidth;
}
```

But the generated packer API already exposes a `pack32Values(...)` entry point for exactly
this case, and the current format always flushes mini blocks as full 32-value groups.

That means each mini block was paying for four virtual calls and four loop iterations when
one call would do.

#### Fix

Replace the inner `pack8Values` loop with a single `pack32Values` call and a fixed byte count:

```java
packer.pack32Values(deltaBlockBuffer, miniBlockStart, miniBlockByteBuffer, 0);
baos.write(miniBlockByteBuffer, 0, currentBitWidth * 4);
```

This was applied to both the integer and long delta writers so the change helps both the
direct `encodeDelta` benchmark and the length side of `DeltaLengthByteArrayValuesWriter`.

#### Benchmark Results

Primary target:

| Benchmark | Pattern | Before | After | Change |
|-----------|---------|--------|-------|--------|
| `IntEncodingBenchmark.encodeDelta` | SEQUENTIAL | 72,386,638 | 74,189,071 | **+2.5%** |
| `IntEncodingBenchmark.encodeDelta` | RANDOM | 49,024,283 | 51,387,152 | **+4.8%** |
| `IntEncodingBenchmark.encodeDelta` | LOW_CARDINALITY | 57,774,044 | 61,434,736 | **+6.3%** |
| `IntEncodingBenchmark.encodeDelta` | HIGH_CARDINALITY | 71,961,199 | 73,436,517 | **+2.0%** |

Average across all patterns: **~62.8M → ~65.1M ops/s (+3.7%)**

Adjacent benchmark checked because it reuses the same delta writer path for lengths:

| Benchmark | Cardinality | StringLength | Before | After | Change |
|-----------|------------|-------------|--------|-------|--------|
| `encodeDeltaLengthByteArray` | LOW | 10 | 20,968,152 | 21,599,885 | **+3.0%** |
| `encodeDeltaLengthByteArray` | LOW | 100 | 6,513,995 | 6,742,292 | **+3.5%** |
| `encodeDeltaLengthByteArray` | LOW | 1000 | 844,501 | 834,686 | -1.2% |
| `encodeDeltaLengthByteArray` | HIGH | 10 | 19,833,467 | 19,137,464 | -3.5% |
| `encodeDeltaLengthByteArray` | HIGH | 100 | 5,798,235 | 5,651,538 | -2.5% |
| `encodeDeltaLengthByteArray` | HIGH | 1000 | 694,440 | 713,940 | **+2.8%** |

The direct target (`encodeDelta`) improved consistently. `encodeDeltaLengthByteArray` was mixed,
which is expected because delta-length encoding combines the delta int path with the dominant
binary payload write cost. The change was kept based on the clear positive result in the direct
delta-int benchmark.

---

## Round 4: Binary Dictionary Encoding Optimization

### Improvement 8: Binary — Cache `hashCode()` for Constant Instances

#### File Changed

`parquet-column/src/main/java/org/apache/parquet/io/api/Binary.java`

#### Problem

`BinaryEncodingBenchmark.encodeDictionary` spends much of its time in dictionary map lookups.
For binary dictionary writers, the hot path is:

```java
int id = binaryDictionaryContent.getInt(v);
```

The benchmark data is generated using `Binary.fromConstantByteArray(...)`, which means the
same immutable `Binary` instances are reused repeatedly in the low-cardinality cases.

However, all `Binary` implementations recomputed `hashCode()` from scratch on every map lookup.
For `ByteArrayBackedBinary`, that meant scanning the entire byte array each time. For long strings,
this dominated encodeDictionary throughput.

#### Fix

Added a transient cached hash code to the `Binary` base class and reused it only for
constant (non-reused) instances:

```java
private transient int cachedHashCode;
private transient boolean hashCodeCached;
```

Each concrete `hashCode()` implementation now does:

```java
if (isHashCodeCached()) {
    return getCachedHashCode();
}
return cacheHashCode(computedHashCode);
```

The cache is only populated when `isBackingBytesReused == false` so behavior remains correct for
reused/mutable backing data. This preserves existing semantics while making repeated dictionary
lookups for constant values effectively O(1) after the first hash.

#### Benchmark Results

| Benchmark | Cardinality | StringLength | Before | After | Change |
|-----------|------------|-------------|--------|-------|--------|
| `encodeDictionary` | LOW | 10 | 13,027,210 | 18,702,494 | **+43.6%** |
| `encodeDictionary` | LOW | 100 | 2,964,740 | 17,298,359 | **+483%** |
| `encodeDictionary` | LOW | 1000 | 295,378 | 19,141,624 | **+6381%** |
| `encodeDictionary` | HIGH | 10 | 826,892 | 1,023,924 | **+23.8%** |
| `encodeDictionary` | HIGH | 100 | 392,497 | 1,161,194 | **+196%** |
| `encodeDictionary` | HIGH | 1000 | 71,879 | 1,195,914 | **+1564%** |

The largest gains occur for long strings and dictionary encode in general, where repeated hashing
cost dominated the entire benchmark. This optimization improves all constant `Binary` map lookup
scenarios, not just dictionary encoding.

---

## Updated Summary of All Results

| # | Optimization | Module | Benchmark | Improvement |
|---|---|---|---|---|
| 1 | BSS writer: inline scatter | parquet-column | encodeByteStreamSplit | **+19.2%** |
| 2 | RLE decoder: buffer reuse | parquet-column | FileReadBenchmark | **~2.2%** |
| 3 | BSS reader: cache-friendly loop | parquet-column | decodeByteStreamSplit | **+87.2%** |
| 4 | LE output: bulk writeInt/writeShort | parquet-common | encodePlain | **+35%** |
| 5 | DeltaBA writer: avoid array copy | parquet-column | encodeDeltaByteArray | **+21.6%** |
| 6 | Delta reader: one slice per miniblock | parquet-column | decodeDelta | **+14%** |
| 7 | Delta writers: use pack32Values | parquet-column | encodeDelta | **+3.7%** |
| 8 | Binary: cache hashCode for constants | parquet-column | encodeDictionary | **+43.6% to +6381%** |

### Test Results

- **parquet-common**: 308 tests, 0 failures, 0 errors
- **parquet-column**: 573 tests, 0 failures, 0 errors

### Files Modified (Round 2)

1. `parquet-common/src/main/java/org/apache/parquet/bytes/LittleEndianDataOutputStream.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/deltastrings/DeltaByteArrayWriter.java`

### Files Modified (Round 3)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/delta/DeltaBinaryPackingValuesReader.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/delta/DeltaBinaryPackingValuesWriterForInteger.java`
3. `parquet-column/src/main/java/org/apache/parquet/column/values/delta/DeltaBinaryPackingValuesWriterForLong.java`

### Files Modified (Round 4)

1. `parquet-column/src/main/java/org/apache/parquet/io/api/Binary.java`

### Commits

```
136c751f1 Optimize encoding hot paths: ByteStreamSplit writer/reader and RLE decoder
b9a2bc794 Optimize plain int encoding and delta byte array writing
dfcab6420 Reduce delta decode ByteBuffer slicing overhead
4cc922e5d Use 32-value packer entry points in delta writers
0baf1e664 Cache hash codes for constant Binary values
```
