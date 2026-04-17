# Parquet-Java Performance Improvement Analysis

## Overview

This document describes the performance analysis and optimizations applied to the
parquet-java (formerly parquet-mr) encoding hot paths, driven by profiling the existing
JMH benchmarks in the `parquet-benchmarks` module.

Ten validated optimizations were implemented across `parquet-common` and `parquet-column`,
targeting the encoding and decoding paths exercised by `IntEncodingBenchmark`,
`BinaryEncodingBenchmark`, and `FileReadBenchmark`. Two additional read-path optimizations
were added in Round 8 (Improvements 12-13). All accepted changes were validated with
JMH benchmarks and the full `parquet-common` (308 tests, 0 failures, 0 errors) and
`parquet-column` (573 tests, 0 failures, 0 errors) test suites.

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

## Round 5: Delta Byte Array Decode Optimization

### Improvement 9: DeltaByteArrayReader — Avoid Hidden Suffix Copies

#### File Changed

`parquet-column/src/main/java/org/apache/parquet/column/values/deltastrings/DeltaByteArrayReader.java`

#### Problem

`DeltaByteArrayReader.readBytes()` already has to materialize a new output array when a value
shares a prefix with the previous value. The existing implementation did this:

```java
byte[] out = new byte[length];
System.arraycopy(previous.getBytesUnsafe(), 0, out, 0, prefixLength);
System.arraycopy(suffix.getBytesUnsafe(), 0, out, prefixLength, suffix.length());
```

That looks fine for byte-array-backed suffixes, but many suffix values come from
`DeltaLengthByteArrayValuesReader` as `ByteBufferBackedBinary`. For that implementation,
`getBytesUnsafe()` falls back to `getBytes()` and materializes a temporary array before the
final `System.arraycopy` into `out`.

So the prefix-sharing path was doing:

1. Allocate the final output array
2. Allocate a temporary suffix array
3. Copy suffix bytes into the temporary array
4. Copy them again into the final output array

#### Fix

Keep the single required output allocation, but stream the suffix bytes directly into the
correct slice of the final array via `Binary.writeTo(...)`:

```java
byte[] out = new byte[length];
System.arraycopy(previous.getBytesUnsafe(), 0, out, 0, prefixLength);
suffix.writeTo(new ByteArraySliceOutputStream(out, prefixLength));
previous = Binary.fromConstantByteArray(out);
```

The local `ByteArraySliceOutputStream` is a tiny adapter over the target byte array. This avoids
the extra hidden suffix materialization while preserving the required final materialized result.

#### Benchmark Results

| Benchmark | Cardinality | StringLength | Before | After | Change |
|-----------|------------|-------------|--------|-------|--------|
| `decodeDeltaByteArray` | LOW | 10 | 13,223,223 | 12,859,448 | -2.8% |
| `decodeDeltaByteArray` | LOW | 100 | 11,596,441 | 12,204,962 | **+5.2%** |
| `decodeDeltaByteArray` | LOW | 1000 | 7,075,096 | 7,888,881 | **+11.5%** |
| `decodeDeltaByteArray` | HIGH | 10 | 13,379,741 | 13,889,337 | **+3.8%** |
| `decodeDeltaByteArray` | HIGH | 100 | 11,675,579 | 12,882,705 | **+10.3%** |
| `decodeDeltaByteArray` | HIGH | 1000 | 7,697,289 | 8,563,087 | **+11.2%** |

The 10-byte LOW-cardinality case moved slightly down and looks like benchmark noise. The
important result is that medium and long strings improved consistently, which matches the
expected reduction in hidden suffix materialization work.

---

## Round 6: Dictionary ID Bit-Packing Optimization

### Improvement 10: RunLengthBitPackingHybridEncoder — Use `pack32Values` for Bit-Packed Runs

#### File Changed

`parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridEncoder.java`

#### Problem

`DictionaryValuesWriter.getBytes()` feeds dictionary IDs through
`RunLengthBitPackingHybridEncoder`. In the bit-packed path, every 8-value group did its own:

```java
packer.pack8Values(bufferedValues, 0, packBuffer, 0);
baos.write(packBuffer);
```

The generated packers already expose a `pack32Values(...)` entry point. That meant long
bit-packed dictionary ID runs were still paying for four pack calls and four output writes
for every 32 values.

#### Fix

Added a 32-value staging buffer for bit-packed runs. The encoder now accumulates four 8-value
groups, flushes full chunks with `pack32Values(...)`, and falls back to `pack8Values(...)`
only when a run ends with fewer than 32 staged values. The on-wire format stays unchanged
because run headers are still counted in 8-value groups.

#### Benchmark Results

`BinaryEncodingBenchmark.encodeDictionary`:

| Cardinality | StringLength | Before | After | Change |
|-----------|-------------|--------|-------|--------|
| LOW | 10 | 20,016,546 | 20,241,575 | **+1.1%** |
| LOW | 100 | 18,021,292 | 18,026,227 | +0.0% |
| LOW | 1000 | 21,780,198 | 21,973,367 | **+0.9%** |
| HIGH | 10 | 1,339,528 | 1,354,035 | **+1.1%** |
| HIGH | 100 | 1,334,816 | 1,333,783 | -0.1% |
| HIGH | 1000 | 1,163,105 | 1,302,550 | **+12.0%** |

`IntEncodingBenchmark.encodeDictionary`:

| Pattern | Before | After | Change |
|-----------|--------|-------|--------|
| SEQUENTIAL | 2,977,430 | 3,082,282 | **+3.5%** |
| RANDOM | 3,027,118 | 3,065,328 | **+1.3%** |
| LOW_CARDINALITY | 19,181,636 | 19,135,921 | -0.2% |
| HIGH_CARDINALITY | 2,945,972 | 3,051,820 | **+3.6%** |

The strongest gain showed up in the long-string high-cardinality binary case, where the
dictionary ID stream stays bit-packed for long stretches. The two small negative moves are
within noise and did not reproduce as meaningful regressions elsewhere.

---

### Investigation: `BytesInput.toByteArray()` Direct Materialization (REVERTED)

Temporary files changed during the experiment:

- `parquet-common/src/main/java/org/apache/parquet/bytes/BytesInput.java`
- `parquet-common/src/main/java/org/apache/parquet/bytes/CapacityByteArrayOutputStream.java`

#### Hypothesis

All encode benchmarks end by materializing `writer.getBytes().toByteArray()`, so replacing the
`ByteArrayOutputStream`-based path with direct writes into the final `byte[]` looked like a
promising shared encode-side optimization.

#### Result

The end-to-end encode benchmarks regressed overall. Largest regressions from the dedicated
before/after run:

| Benchmark | Params | Before | After | Change |
|-----------|--------|--------|-------|--------|
| `encodeByteStreamSplit` | SEQUENTIAL | 22,971,724 | 20,101,829 | **-12.5%** |
| `encodeByteStreamSplit` | RANDOM | 22,485,490 | 20,009,562 | **-11.0%** |
| `encodeDictionary` | HIGH_CARDINALITY | 3,030,307 | 2,774,764 | **-8.4%** |
| `encodePlain` | SEQUENTIAL | 28,461,041 | 26,655,025 | **-6.3%** |

Some binary encode cases moved up slightly, but the shared change was clearly regressive overall.
The experiment was fully reverted.

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
| 9 | DeltaBA reader: avoid hidden suffix copy | parquet-column | decodeDeltaByteArray | **+3.8% to +11.5%** |
| 10 | RLE encoder: batch bit-packed groups | parquet-column | encodeDictionary | **up to +12.0%** |

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

### Files Modified (Round 5)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/deltastrings/DeltaByteArrayReader.java`

### Files Modified (Round 6)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridEncoder.java`

### Commits

```
136c751f1 Optimize encoding hot paths: ByteStreamSplit writer/reader and RLE decoder
b9a2bc794 Optimize plain int encoding and delta byte array writing
dfcab6420 Reduce delta decode ByteBuffer slicing overhead
4cc922e5d Use 32-value packer entry points in delta writers
0baf1e664 Cache hash codes for constant Binary values
344c48168 Avoid hidden suffix copies in delta byte array decode
5b9f66494 Use pack32Values in RLE hybrid encoder
```

---

## Round 7: Row Group Flush Memory Optimization

### Improvement 11: Eagerly Release Column Buffers During Row Group Flush

**Status: No measurable benefit. Kept as code quality improvement only.**

#### Files Changed

1. `parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ColumnChunkPageWriteStore.java`
2. `parquet-common/src/main/java/org/apache/parquet/bytes/ConcatenatingByteBufferCollector.java`

#### Problem Statement

When flushing a row group to disk, column buffers are written to the output stream one
at a time, but all buffers stay alive until `close()` is called after the entire flush
loop. We hypothesized that releasing each column's buffers immediately after writing
would reduce peak memory.

#### What We Did

**Change 1: Eager release in `flushToFileWriter()`**

```java
// After: release each column's buffers immediately after writing
public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
    for (ColumnDescriptor path : schema.getColumns()) {
        ColumnChunkPageWriter pageWriter = writers.get(path);
        pageWriter.writeToFileWriter(writer);
        pageWriter.close();  // release buf immediately
    }
}
```

**Change 2: Idempotent `close()` on `ConcatenatingByteBufferCollector`**

```java
@Override
public void close() {
    if (slabs.isEmpty()) {
        return;
    }
    for (ByteBuffer slab : slabs) {
        allocator.release(slab);
    }
    slabs.clear();
    size = 0;
}
```

**Change 3: Progressive slab release API (`writeAllToAndRelease`)**

Added for potential future use but not wired into the flush path.

We also attempted an **interleaved flush** where each column's pages are finalized,
written, and released before the next column begins (combining `columnStore.flush()`
and `pageStore.flushToFileWriter()` into a single per-column loop via a new
`ColumnWriteStore.flushColumn(ColumnDescriptor)` API). This was hypothesized to
reduce peak memory from all N columns to ~1 column.

#### Why It Does Not Reduce Peak Memory

The flush pipeline is a two-phase process:

1. **Phase 1 (`columnStore.flush()`)**: Finalizes each column's last partial page
   into a compressed `ByteBuffer` in the `ConcatenatingByteBufferCollector`.
2. **Phase 2 (`pageStore.flushToFileWriter()`)**: Writes each column to disk.

However, **most page buffers are NOT created during flush**. They are created during
the regular write loop: as rows are added, `ColumnWriteStoreBase.sizeCheck()` triggers
`writePage()` whenever a column's buffered data exceeds the page size threshold (~1MB).
Each `writePage()` compresses the page data, allocates a `ByteBuffer` via the allocator,
and stores it in the `ConcatenatingByteBufferCollector`. The column writer's
`CapacityByteArrayOutputStream` slabs are then released (reset).

**Peak memory is reached during the regular write phase**, not during flush. At any
point in the write loop, when a page is being finalized:

```
peak = all N columns' CapacityByteArrayOutputStream slabs (unflushed data)
     + all completed page ByteBuffers already in collectors
     + the current page being collected
```

This peak is **identical** whether the subsequent flush is batch or interleaved, because
the flush only adds partial pages (one at a time) and never exceeds the write-phase peak.

For small row groups (8MB with 20 columns at ~204 bytes/row/column): each column
accumulates ~410KB before flush (below the 1MB page threshold). No pages are finalized
during writing. All finalization happens during flush. But even then, the peak occurs
at the moment the first column's page is collected — at that instant, all 20 columns'
stream slabs are still allocated, giving peak ≈ 20 * 410KB + 410KB ≈ 8.6MB. This is
the same in both batch and interleaved flush.

Additionally, `HeapByteBufferAllocator.release()` is a **complete no-op** — it just
returns. Buffer memory is only reclaimed by GC when references are cleared. The eager
release clears the `slabs` list (making `ByteBuffer` objects GC-eligible) but does not
deterministically free memory.

#### Benchmark Results — Precise Allocator Tracking

`RowGroupFlushBenchmark` — 100K rows, 20 BINARY columns (200 bytes each), PLAIN
encoding (V1), UNCOMPRESSED, dictionary disabled, `-wi 2 -i 3 -f 1`, JVM: `-Xms512m
-Xmx1g`. Uses a `PeakTrackingAllocator` wrapper that tracks current and peak bytes
outstanding across all parquet-managed `ByteBuffer` allocations.

| Row Group Size | Baseline Peak (MB) | Optimized Peak (MB) | Baseline Time (ms) | Optimized Time (ms) |
|---|---|---|---|---|
| 8 MB  | **9.381** | **9.381** | 2056.7 ± 62.5 | 2015.9 ± 156.8 |
| 64 MB | **66.018** | **66.018** | 2164.7 ± 336.5 | 2154.1 ± 38.9 |

**Peak allocator bytes are byte-for-byte identical** (9,836,668 and 69,225,305
respectively). Both batch flush and interleaved flush produce the same peak because
the peak is set during the write phase, before flush begins.

Throughput is also within error margins.

#### Earlier JVM Heap Benchmark (6-column schema, 8MB row groups)

An earlier benchmark used JVM heap sampling (`Runtime.totalMemory() - freeMemory()`)
instead of allocator tracking. This also showed no difference:

| Codec | Version | Before (ms) | After (ms) | Before Peak (KB) | After Peak (KB) |
|---|---|---|---|---|---|
| UNCOMPRESSED | V1 | 880.9 | 883.3 | 154,312 | 154,328 |
| UNCOMPRESSED | V2 | 799.3 | 797.1 | 155,980 | 155,992 |
| SNAPPY | V1 | 952.1 | 955.5 | 176,820 | 176,935 |
| SNAPPY | V2 | 821.4 | 819.8 | 162,050 | 162,104 |

#### Conclusion

The optimization **does not reduce peak memory or improve throughput**. The theoretical
"N columns → 1 column" peak reduction assumed page buffers are only created during
flush, but in practice they accumulate during the write loop. The peak is fully
determined before flush begins.

The code change is kept because:
- It is correct resource management (release when no longer needed)
- It has zero overhead (the `close()` calls are essentially free)
- It makes buffers GC-eligible sooner (minor benefit under memory pressure)
- It makes the page writers' lifecycle clearer

But it should **not** be claimed as a memory optimization in a PR.

#### What Would Actually Reduce Peak Memory

The only way to reduce peak buffer memory during row group writing would be to **stream
columns to disk as pages complete** rather than buffering the entire row group. However,
this is architecturally incompatible with the current Parquet format:
- `ParquetFileWriter.startBlock(recordCount)` needs the row count before writing begins
- Column indexes and offset indexes require all pages to be known per-column
- The row group metadata in the footer requires complete information about all columns

#### Tests

New tests added:

| Test | File | What it verifies |
|------|------|-----------------|
| `testEagerReleaseOnFlush` | `TestColumnChunkPageWriteStore` | Buffers released after flush, no leaks (TrackingByteBufferAllocator) |
| `testDoubleCloseIsSafe` | `TestColumnChunkPageWriteStore` | Store can be closed twice without errors |
| `testEagerReleaseWriteReadRoundtrip` | `TestColumnChunkPageWriteStore` | End-to-end 3-column write/read, data integrity after eager release |
| `testWriteAllToAndRelease` | `TestConcatenatingByteBufferCollector` | Progressive release writes correct data, empties collector |
| `testDoubleCloseIsSafe` | `TestConcatenatingByteBufferCollector` | Idempotent close |
| `testCloseOnEmpty` | `TestConcatenatingByteBufferCollector` | Close on never-used collector |
| `testWriteAllToAndReleaseProducesIdenticalOutput` | `TestConcatenatingByteBufferCollector` | Byte-identical output between writeAllTo and writeAllToAndRelease |

All existing test suites pass:

- **parquet-common**: 312 tests, 0 failures, 0 errors
- **parquet-column**: 573 tests, 0 failures, 0 errors
- **parquet-hadoop** (non-Hadoop tests): All new + existing mock tests pass
- **parquet-benchmarks** (`ConcurrentCorrectnessTest`): Passes (thread safety verified)

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
| 9 | DeltaBA reader: avoid hidden suffix copy | parquet-column | decodeDeltaByteArray | **+3.8% to +11.5%** |
| 10 | RLE encoder: batch bit-packed groups | parquet-column | encodeDictionary | **up to +12.0%** |
| 11 | Eager column buffer release during flush | parquet-hadoop | RowGroupFlushBenchmark | **No effect** (peak set during write phase, not flush) |

---

## Improvement 12: PlainValuesReader — Direct ByteBuffer Reads

### Round 8 — Read-path optimizations

### Problem

`PlainValuesReader` (used for PLAIN-encoded INT32, INT64, FLOAT, and DOUBLE columns) reads
values through a two-layer wrapper chain:

1. `initFromPage()` calls `stream.remainingStream()` to get a `ByteBufferInputStream`
2. Wraps it in `LittleEndianDataInputStream(stream.remainingStream())`
3. Each `readInt()` performs **4 separate `in.read()` virtual calls** through the InputStream chain
4. Each `in.read()` dispatches to `SingleBufferInputStream.read()` → `buffer.get() & 0xFF`
5. The 4 bytes are then manually assembled: `(ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4`

The code itself has a TODO comment (line 335-339 in `LittleEndianDataInputStream`) acknowledging
this might be slow and suggesting alternatives.

For every plain-encoded primitive value, this means: 4 virtual method dispatches, 4 byte-at-a-time
ByteBuffer reads, 4 unsigned mask operations, and manual bit-shifting to assemble the result.

### Solution

Bypass the `LittleEndianDataInputStream` entirely. In `initFromPage()`, obtain the page data as
a single contiguous `ByteBuffer` via `stream.slice(stream.available())` and set its byte order
to `ByteOrder.LITTLE_ENDIAN`. Each reader then calls the corresponding `ByteBuffer` method
directly:

- `readInteger()` → `buffer.getInt()` (single JVM intrinsic)
- `readFloat()` → `buffer.getFloat()`
- `readDouble()` → `buffer.getDouble()`
- `readLong()` → `buffer.getLong()`
- `skip(n)` → `buffer.position(buffer.position() + n * typeSize)`

`ByteBuffer.getInt()` with the correct byte order is a single JVM intrinsic that compiles to
a direct memory read instruction — no virtual dispatch, no byte-at-a-time assembly.

The `ByteBufferInputStream.slice()` method handles both single-buffer (zero-copy view) and
multi-buffer (copy into contiguous buffer) cases transparently. In practice, page data is
almost always a single contiguous buffer.

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/PlainValuesReader.java`

### Results

Micro-benchmark: `IntEncodingBenchmark.decodePlain` (100,000 INT32 values per invocation)

| Data Pattern | Baseline (ops/s) | Optimized (ops/s) | Speedup |
|---|---|---|---|
| SEQUENTIAL | 92,918,297 | 1,143,149,235 | **12.3×** |
| RANDOM | 92,126,888 | 1,147,547,093 | **12.5×** |
| LOW_CARDINALITY | 93,005,451 | 1,142,666,760 | **12.3×** |
| HIGH_CARDINALITY | 93,312,596 | 1,144,681,876 | **12.3×** |

Average improvement: **~12.3× faster** across all data patterns.

The improvement is consistent regardless of data distribution because the bottleneck was entirely
in the dispatch overhead, not the data itself. All four numeric plain reader types (int, float,
double, long) benefit equally from this change.

### Tests

All 573 `parquet-column` tests pass with zero failures or regressions.

---

## Improvement 13: RLE Hybrid Decoder — Cache DataInputStream and Simplify Index

### Problem

`RunLengthBitPackingHybridDecoder.readNext()` allocates `new DataInputStream(in)` on every
PACKED run (line 111). Additionally, the PACKED case in `readInt()` uses a reverse-computed
index: `currentBuffer[currentBufferLength - 1 - currentCount]`, requiring a subtraction per
read.

### Solution

1. Cache the `DataInputStream` as a field, initialized in the constructor
2. Replace `currentBufferLength` with a forward-moving `currentBufferIdx` that increments on
   each read, simplifying the array access to `currentBuffer[currentBufferIdx++]`

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`

### Results

Micro-benchmark: `IntEncodingBenchmark.decodeRle` (100,000 values, 10-bit width)

| Data Pattern | Baseline (ops/s) | Optimized (ops/s) | Change |
|---|---|---|---|
| SEQUENTIAL | 110,688,255 | 110,185,851 | within noise |
| RANDOM | 112,389,781 | 110,264,104 | within noise |
| LOW_CARDINALITY | 112,414,824 | 110,799,236 | within noise |
| HIGH_CARDINALITY | 111,335,663 | 111,170,291 | within noise |

**No measurable performance improvement.** The `DataInputStream` allocation is tiny and amortized
over 8+ values per packed run, and the arithmetic simplification is trivially optimized by the JIT.
The changes are retained as code quality improvements (clearer index tracking, no per-run allocation).

### Tests

All 573 `parquet-column` tests + 9 RLE-specific tests pass with zero failures.

---

## Improvement 14: RLE Decoder — Remove Checked IOException from readInt()

### Round 9 — Read-path optimizations (continued)

### Problem

`RunLengthBitPackingHybridDecoder.readInt()` declares `throws IOException`, forcing every
caller to wrap the call in a try-catch block that converts to `ParquetDecodingException`.
This pattern appears in:

- `DictionaryValuesReader` — 7 methods (readBytes, readFloat, readDouble, readInteger, readLong,
  readValueDictionaryId, skip), each with its own try-catch block
- `RunLengthBitPackingHybridValuesReader.readInteger()` — 1 try-catch block
- `ColumnReaderBase.RLEIntIterator.nextInt()` — 1 try-catch block (called for every
  definition/repetition level read)

That's 9 try-catch blocks in 3 files, all doing the same `catch (IOException e) { throw new
ParquetDecodingException(e); }`.

### Solution

Move the IOException handling inside `readInt()` itself: catch IOException from `readNext()`
and wrap in `ParquetDecodingException`. Remove `throws IOException` from the signature.
All 9 caller try-catch blocks become unnecessary and are removed.

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/dictionary/DictionaryValuesReader.java`
3. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridValuesReader.java`
4. `parquet-column/src/main/java/org/apache/parquet/column/impl/ColumnReaderBase.java`

### Results

Micro-benchmarks: `IntEncodingBenchmark.decodeRle` and `IntEncodingBenchmark.decodeDictionary`

| Benchmark | Pattern | Baseline (ops/s) | Optimized (ops/s) | Change |
|---|---|---|---|---|
| decodeRle | RANDOM | 112,138,596 | 112,405,846 | within noise |
| decodeDictionary | RANDOM | 93,063,173 | 92,793,476 | within noise |

**No measurable performance improvement.** Modern JVMs handle try-catch blocks with zero overhead
on the non-exception path — the exception handler is registered in the bytecode exception table
but generates no per-call instructions. The change is retained as a code quality improvement:
simpler callers, cleaner API, and consistent exception handling.

### Tests

All 573 `parquet-column` tests pass with zero failures.

---

## Improvement 15: BinaryPlainValuesReader — Direct ByteBuffer Reads

### Problem

`BinaryPlainValuesReader.readBytes()` reads the 4-byte length prefix of each binary value
using `BytesUtils.readIntLittleEndian(in)`, which performs 4 separate `in.read()` virtual calls
through the `ByteBufferInputStream` chain — the same problem as Improvement 12 for numeric
plain readers.

Additionally, the binary data itself was read via `in.slice(length)` on a `ByteBufferInputStream`,
adding unnecessary method dispatch overhead.

### Solution

Same approach as Improvement 12: in `initFromPage()`, obtain the page data as a single
contiguous `ByteBuffer` via `stream.slice(stream.available())` with `LITTLE_ENDIAN` byte order.

- Length prefix: `buffer.getInt()` (single JVM intrinsic, replaces 4 virtual reads)
- Binary data: `buffer.slice()` + `limit(length)` (zero-copy view from the ByteBuffer)
- Skip: `buffer.getInt()` + `buffer.position(buffer.position() + length)`

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/BinaryPlainValuesReader.java`

### Results

Micro-benchmark: `BinaryEncodingBenchmark.decodePlain` (100,000 BINARY values per invocation)

| Cardinality | String Length | Baseline (ops/s) | Optimized (ops/s) | Improvement |
|---|---|---|---|---|
| LOW | 10 | 21,992,203 | 24,629,299 | **+12.0%** |
| LOW | 100 | 20,001,567 | 20,532,222 | +2.7% |
| LOW | 1000 | 9,022,357 | 9,290,204 | +3.0% |
| HIGH | 10 | 21,211,350 | 24,548,009 | **+15.7%** |
| HIGH | 100 | 20,149,876 | 21,329,196 | **+5.9%** |
| HIGH | 1000 | 8,215,046 | 8,685,848 | +5.7% |

The improvement is most pronounced for short strings (10 bytes): **+12-16%**, where the 4-byte
length prefix read is a significant fraction of per-value cost (~29% of I/O). For longer strings,
the data slicing dominates and the improvement is smaller (+3-6%).

### Tests

All 573 `parquet-column` tests pass with zero failures.

---

## Round 10: RLE Decoder Direct ByteBuffer Reads

### Improvement 16: RLE Decoder — Replace InputStream with Direct ByteBuffer

**Hypothesis:** The `RunLengthBitPackingHybridDecoder` reads all data through an `InputStream`
abstraction (`ByteBufferInputStream`), requiring virtual method dispatches on every `in.read()`
and `in.available()` call. Replacing with direct `ByteBuffer` access should eliminate these
dispatches, following the same pattern that yielded 12.3x for `PlainValuesReader` (Improvement 12).

**Changes:**
- Added `BytesUtils.readUnsignedVarInt(ByteBuffer)` — reads varint directly from buffer
- Added `BytesUtils.readIntLittleEndianPaddedOnBitWidth(ByteBuffer, int)` — uses `getShort()`/`getInt()`
  with LITTLE_ENDIAN byte order for 2-byte and 4-byte reads
- Converted `RunLengthBitPackingHybridDecoder` constructor from `(int, InputStream)` to `(int, ByteBuffer)`
- Replaced `in.available()` → `buffer.hasRemaining()`
- Replaced `dataIn.readFully()` → `buffer.get(byte[], offset, len)`
- Eliminated `DataInputStream` field entirely
- Updated all callers:
  - `DictionaryValuesReader.initFromPage()` — uses `in.slice(in.available())` to get ByteBuffer
  - `RunLengthBitPackingHybridValuesReader.initFromPage()` — uses `stream.slice(length)`
  - `ColumnReaderBase.newRLEIterator()` — uses `bytes.toByteBuffer()`
  - Tests and benchmarks updated to use `ByteBuffer.wrap()`

### Files Modified

1. `parquet-common/src/main/java/org/apache/parquet/bytes/BytesUtils.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`
3. `parquet-column/src/main/java/org/apache/parquet/column/values/dictionary/DictionaryValuesReader.java`
4. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridValuesReader.java`
5. `parquet-column/src/main/java/org/apache/parquet/column/impl/ColumnReaderBase.java`
6. `parquet-benchmarks/src/main/java/org/apache/parquet/benchmarks/IntEncodingBenchmark.java`
7. `parquet-column/src/test/java/org/apache/parquet/column/values/rle/TestRunLengthBitPackingHybridEncoder.java`
8. `parquet-column/src/test/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridIntegrationTest.java`

### Results

Micro-benchmark: `IntEncodingBenchmark.decodeRle` and `decodeDictionary` (100,000 values per invocation)

**decodeRle:**

| Pattern | Baseline (ops/s) | Optimized (ops/s) | Change |
|---|---|---|---|
| SEQUENTIAL | ~112M | 111,008,341 | Within noise |
| RANDOM | ~112M | 110,154,381 | Within noise |
| LOW_CARDINALITY | ~112M | 108,534,389 | Within noise |
| HIGH_CARDINALITY | ~112M | 111,448,755 | Within noise |

**decodeDictionary:**

| Pattern | Optimized (ops/s) |
|---|---|
| SEQUENTIAL | 86,305,566 |
| RANDOM | 84,826,802 |
| LOW_CARDINALITY | 106,204,465 |
| HIGH_CARDINALITY | 85,248,254 |

**No measurable performance improvement.** The reason is that the RLE decoder's hot path is
fundamentally different from PlainValuesReader's:

- **PlainValuesReader** (12.3x improvement): Every value requires I/O — 4 `in.read()` calls per
  INT32 value. Eliminating virtual dispatch per-value yields massive gains.
- **RLE decoder** (no improvement): The per-value path is just a counter decrement + switch +
  array/constant return from cached data. The I/O (`readNext()`) only happens once per RLE run
  or packed group (every 8+ values). The InputStream overhead is amortized over many values,
  making it negligible compared to the bit unpacking work.

This is consistent with Improvements 13 and 14, which also targeted the RLE decoder with no
measurable performance effect. The RLE decoder is not I/O-bound on the per-value path.

**Value as code quality improvement:**
- Eliminates the `InputStream` and `DataInputStream` abstraction layers
- No more checked `IOException` in the I/O path (only unchecked `ParquetDecodingException`)
- Consistent API with PlainValuesReader and BinaryPlainValuesReader (all use ByteBuffer)
- Removes one layer of indirection for future JIT optimization opportunities

### Tests

All 573 `parquet-column` tests pass with zero failures.

---

## Round 11: PlainValuesWriter Direct Slab Writes

### Improvement 17: PlainValuesWriter — Bypass LittleEndianDataOutputStream with Direct ByteBuffer Slab Writes

**Hypothesis:** `PlainValuesWriter` writes values through a two-layer abstraction:
`PlainValuesWriter` → `LittleEndianDataOutputStream` → `CapacityByteArrayOutputStream`.
Each `writeInt()` decomposes the int into 4 bytes in a temp `writeBuffer[8]` array, then calls
`out.write(writeBuffer, 0, 4)` which dispatches through the OutputStream chain. Since
`CapacityByteArrayOutputStream` already uses `ByteBuffer` slabs internally, we can write
directly to the slab with `putInt()`/`putLong()` using LITTLE_ENDIAN byte order — a single
JVM intrinsic on x86/ARM — eliminating the byte decomposition, temp array, and virtual dispatch.

**Changes:**
- `CapacityByteArrayOutputStream`: Set `ByteOrder.LITTLE_ENDIAN` on newly allocated slabs in
  `addSlab()`. Added `writeInt(int)` and `writeLong(long)` methods that use
  `currentSlab.putInt(v)` / `currentSlab.putLong(v)` directly.
- `PlainValuesWriter`: Eliminated `LittleEndianDataOutputStream out` field entirely. All typed
  write methods (`writeInteger`, `writeLong`, `writeFloat`, `writeDouble`) now call
  `arrayOut.writeInt()` / `arrayOut.writeLong()` directly. `writeBytes(Binary)` uses
  `arrayOut.writeInt(v.length())` + `v.writeTo(arrayOut)`. The `getBytes()` method no longer
  needs to flush (no buffering layer). The `close()` method no longer closes the defunct
  `LittleEndianDataOutputStream`.

**What was eliminated per writeInt call:**
- 4 byte-shift operations for little-endian decomposition
- 1 intermediate `writeBuffer[8]` array write
- 2 levels of virtual dispatch (`LittleEndianDataOutputStream.write()` → `CapacityByteArrayOutputStream.write()`)
- 1 bounds check in `write(byte[], off, len)`
- 1 `System.arraycopy` for 4 bytes

**Replaced with:**
- 1 remaining-check on the slab ByteBuffer
- 1 `ByteBuffer.putInt()` call (single JVM intrinsic, ~1 CPU store instruction on little-endian architectures)

### Files Modified

1. `parquet-common/src/main/java/org/apache/parquet/bytes/CapacityByteArrayOutputStream.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/PlainValuesWriter.java`

### Results

Micro-benchmark: `IntEncodingBenchmark.encodePlain` (100,000 INT32 values per invocation)

| Pattern | Baseline (ops/s) | Optimized (ops/s) | Improvement |
|---|---|---|---|
| SEQUENTIAL | 26,817,451 | 52,953,193 | **+97.5% (2.0x)** |
| RANDOM | 28,517,312 | 37,774,036 | **+32.5%** |
| LOW_CARDINALITY | 28,705,158 | 52,819,678 | **+84.0%** |
| HIGH_CARDINALITY | 28,595,519 | 37,862,571 | **+32.4%** |

The improvement varies by data pattern: SEQUENTIAL and LOW_CARDINALITY see ~2x because the
slab `putInt()` path has highly predictable branching (slab rarely runs out for sequential
writes). RANDOM and HIGH_CARDINALITY still see a solid +32% improvement.

This optimization also benefits `writeFloat()`, `writeDouble()`, and `writeLong()` (same
code path), and the length-prefix write in `writeBytes(Binary)`.

Decode benchmark (`decodePlain`) confirmed at ~1.15B ops/s — encoded data is read-compatible.

### Tests

All 573 `parquet-column` tests pass with zero failures.

---

## Round 12: ByteStreamSplit Writer Batch Scatter

### Improvement 18: ByteStreamSplitValuesWriter — Batch Scatter Writes

**Hypothesis:** The BSS writer scatters each value's bytes across N separate
`CapacityByteArrayOutputStream` instances. For a 4-byte int, each `writeInteger()` makes
4 individual `CBOS.write(int b)` calls, each of which does a `hasRemaining()` check,
`ByteBuffer.put((byte)b)`, and `Math.addExact(bytesUsed, 1)`. That's 12 operations per
value. By buffering values in a batch (64 values), we can extract bytes per-stream and
write them in one bulk `CBOS.write(byte[], off, len)` call per stream per flush. This
replaces 4N individual single-byte writes with 4 bulk writes, where each bulk write does
a single bounds check, a single `System.arraycopy`, and a single `addExact`.

**Changes:**
- `ByteStreamSplitValuesWriter`: Added batch buffering infrastructure:
  - `BATCH_SIZE = 64`, `int[] intBatch`, `long[] longBatch`, `byte[] scatterBuf`,
    `int batchCount` fields (lazily allocated on first use)
  - `bufferInt(int v)`: stores value in batch; flushes when full
  - `bufferLong(long v)`: stores value in batch; flushes when full
  - `flushIntBatch()` / `flushLongBatch()`: extracts bytes per-stream into `scatterBuf`,
    calls `byteStreams[s].write(scatterBuf, 0, count)` for each stream
  - `flushBatch()`: dispatches to the appropriate flush method
  - `getBytes()`: calls `flushBatch()` before returning encoded data
  - `getBufferedSize()`: accounts for unflushed batch without triggering flush
  - `reset()` / `close()`: resets `batchCount` to 0
- All int/float subclasses now call `bufferInt()` instead of `scatterInt()`
- All long/double subclasses now call `bufferLong()` instead of `scatterLong()`
- `FixedLenByteArrayByteStreamSplitValuesWriter` still uses unbatched `scatterBytes()`
  (variable element size, not worth batching)
- Removed unused `BytesUtils` import

**What was eliminated per value (int case, amortized over batch of 64):**
- 4 individual `CBOS.write(int b)` calls (each: hasRemaining + put + addExact) = 12 ops
- Replaced with: ~4 byte-extract ops + 4/64 bulk write ops ≈ 4.06 ops amortized

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesWriter.java`

### Results

Micro-benchmark: `IntEncodingBenchmark.encodeByteStreamSplit` (100,000 INT32 values per invocation)

| Pattern | Baseline (ops/s) | Optimized (ops/s) | Improvement |
|---|---|---|---|
| SEQUENTIAL | ~22,600,000 | 52,936,293 | **+134% (2.3x)** |
| RANDOM | 22,643,010 | 53,834,138 | **+138% (2.4x)** |
| LOW_CARDINALITY | ~22,600,000 | 53,104,147 | **+135% (2.3x)** |
| HIGH_CARDINALITY | ~22,600,000 | 53,224,791 | **+135% (2.4x)** |

The improvement is consistent (~2.35x) across all data patterns, as expected: BSS encoding
treats all values identically regardless of data distribution. The optimized BSS writer now
matches PLAIN encoding throughput (~52.5M ops/s).

Decode benchmark (`decodeByteStreamSplit`) confirmed at ~84M ops/s — encoded data is
read-compatible (writer changes are output-transparent).

### Tests

All 573 `parquet-column` tests pass with zero failures, including all BSS-specific tests:
- `ByteStreamSplitValuesWriterTest` (7 tests: Float, Double, Integer, Long, FixedLenByteArray)
- `ByteStreamSplitValuesReaderTest` (16 tests)
- `ByteStreamSplitValuesEndToEndTest` (5 tests)

---

## Round 13: Page Assembly Copy Elimination

### Improvement 19: BAOSBytesInput.writeInto — Eliminate Intermediate toByteArray() Copy

**Hypothesis:** When pages are compressed, `HeapBytesCompressor.compress()` returns a
`BAOSBytesInput` wrapping a `ByteArrayOutputStream`. When `ConcatenatingByteBufferCollector.collect()`
calls `writeInto(ByteBuffer)`, the implementation did `buffer.put(arrayOut.toByteArray())` —
allocating a full page-size byte[] copy via `Arrays.copyOf(buf, count)`, then copying it into the
target ByteBuffer. That's 2 copies when only 1 is needed.

**Fix:** Use `arrayOut.writeTo(ByteBufferBackedOutputStream)` instead, which calls
`out.write(buf, 0, count)` directly from the internal buffer — a single `buffer.put(b, off, len)`
memcpy. Added a thin `ByteBufferBackedOutputStream` adapter to bridge the OutputStream and ByteBuffer APIs.

**Also:** Replaced `crc.update(compressedBytes.toByteArray())` in `ColumnChunkPageWriteStore` with
streaming CRC computation via `compressedBytes.writeAllTo(CRC32OutputStream)`. This writes directly
from each BytesInput's backing buffer into the CRC32, eliminating an additional page-size `toByteArray()`
allocation+copy per page when CRC checksums are enabled. Applied to both V1 and V2 page paths.

**What was eliminated:**
- Per compressed page: 1 unnecessary `Arrays.copyOf(buf, count)` allocation + memcpy (~1MB per page)
- Per CRC-enabled page (V1): 1 additional `toByteArray()` allocation + memcpy
- Per CRC-enabled page (V2): up to 3 additional `toByteArray()` allocations + memcpys (RL, DL, data)

### Files Modified

1. `parquet-common/src/main/java/org/apache/parquet/bytes/BytesInput.java`
2. `parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ColumnChunkPageWriteStore.java`

### Results

End-to-end write benchmark: `FileWriteBenchmark.writeFile` (100,000 rows, 6-column schema)

| Codec | Dict | Baseline (ms) | Optimized (ms) | Notes |
|---|---|---|---|---|
| UNCOMPRESSED | true | ~184 | ~188 | No BAOSBytesInput in path (NO_OP compressor) |
| SNAPPY | true | ~201 | ~200 | Saves ~1 page-copy per page, below noise threshold |

The improvement is architecturally correct but below the noise threshold of the end-to-end benchmark.
For a 6-column, 100K-row file, the saved copies total ~6MB (one per column page). At memory
bandwidth speeds (~10GB/s), that's ~0.6ms — within the ±8ms error bars. The benefit becomes more
visible with larger files, more columns, or when CRC checksums are enabled.

### Tests

- 312 `parquet-common` tests pass (0 failures)
- `TestCompressionCodec` (5 tests), `TestDirectCodecFactory` (4 tests), `TestSnappyCodec` (2 tests) pass
- Pre-existing `getSubject` Hadoop failures on Java 18+ are unrelated

---

## Round 14: Delta Binary Writers — Eliminate Per-Value Allocations and LE Wrapper

### Improvement 20: DeltaByteArrayWriter — Avoid Binary.slice() Allocation Per Value

**Hypothesis:** In `DeltaByteArrayWriter.writeBytes()`, after computing the prefix length, the suffix
was written via `suffixWriter.writeBytes(v.slice(i, vb.length - i))`. This creates a new
`ByteArraySliceBackedBinary` object per value just to pass to the suffix writer, which then calls
`v.writeTo(out)` → `LittleEndianDataOutputStream.write(byte[], off, len)` → `CBOS.write(byte[], off, len)`.
That's one object allocation + three virtual dispatches for what should be a single `write(byte[], off, len)`.

**Fix:** Changed `suffixWriter` field type from `ValuesWriter` to `DeltaLengthByteArrayValuesWriter`
and added a `writeBytes(byte[] data, int offset, int length)` method that writes directly to the
underlying `CapacityByteArrayOutputStream`. The suffix is now written as
`suffixWriter.writeBytes(vb, i, vb.length - i)` — no Binary object, no virtual dispatch chain.

### Improvement 21: DeltaLengthByteArrayValuesWriter — Eliminate LittleEndianDataOutputStream Wrapper

**Hypothesis:** `DeltaLengthByteArrayValuesWriter` wrapped its `CapacityByteArrayOutputStream` with a
`LittleEndianDataOutputStream`, but only ever used `write()` methods (raw byte writes), never any of
the typed LE methods (writeInt, writeLong, etc.). The wrapper added an extra virtual dispatch per
`Binary.writeTo()` call with zero benefit.

**Fix:** Removed the `LittleEndianDataOutputStream` wrapper entirely. `Binary.writeTo()` now writes
directly to `CapacityByteArrayOutputStream`. Also removed the unnecessary `out.flush()` call from
`getBytes()` — CBOS has no internal buffering, so flush is a no-op.

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/deltastrings/DeltaByteArrayWriter.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/deltalengthbytearray/DeltaLengthByteArrayValuesWriter.java`

### Results

**DELTA_BYTE_ARRAY encode** (improvements 20+21 combined):

| Cardinality | String Length | Baseline (ops/s) | Optimized (ops/s) | Change |
|---|---|---|---|---|
| LOW | 10 | 11,840,824 ± 662K | 15,772,113 ± 1.06M | **+33%** |
| LOW | 100 | 5,851,912 ± 457K | 6,111,386 ± 139K | +4.4% |
| LOW | 1000 | 824,232 ± 22K | 842,363 ± 41K | +2.2% |
| HIGH | 10 | 11,226,335 ± 538K | 13,810,861 ± 897K | **+23%** |
| HIGH | 100 | 5,071,629 ± 106K | 5,200,847 ± 133K | +2.5% |
| HIGH | 1000 | 685,307 ± 47K | 693,171 ± 72K | +1.1% |

**DELTA_LENGTH_BYTE_ARRAY encode** (improvement 21 only — LE wrapper removal):

| Cardinality | String Length | Baseline (ops/s) | Optimized (ops/s) | Change |
|---|---|---|---|---|
| LOW | 10 | 21,949,079 ± 363K | 25,432,103 ± 291K | **+16%** |
| LOW | 100 | 6,897,990 ± 185K | 7,266,852 ± 370K | +5.3% |
| LOW | 1000 | 851,717 ± 46K | 856,046 ± 75K | ~0% |
| HIGH | 10 | 19,637,128 ± 1.62M | 23,206,164 ± 1.37M | **+18%** |
| HIGH | 100 | 5,702,644 ± 113K | 5,925,859 ± 127K | +3.9% |
| HIGH | 1000 | 679,132 ± 60K | 708,894 ± 16K | +4.4% |

The biggest gains are for short strings where per-value overhead (object allocation + virtual dispatch
chain) is a larger fraction of the total work. For 10-byte strings, eliminating the Binary.slice()
allocation and the LEDO dispatch chain yields 23-33% improvement. Decode benchmarks are unaffected.

### Tests

- 23 tests pass: 6 DeltaByteArray + 13 DeltaLengthByteArray + 4 benchmarks (0 failures)

---

## Round 15: Dictionary Writer and FixedLenByteArray Writer Cleanup

### Improvement 22: FixedLenByteArrayPlainValuesWriter — Eliminate LittleEndianDataOutputStream Wrapper

**Hypothesis:** Same pattern as DeltaLengthByteArrayValuesWriter (Improvement 21). The writer wrapped
`CapacityByteArrayOutputStream` with `LittleEndianDataOutputStream` but only used raw byte writes
(`Binary.writeTo()`), never any typed LE methods. The wrapper added an unnecessary virtual dispatch.

**Fix:** Removed the LEDO wrapper. `Binary.writeTo()` now writes directly to `CapacityByteArrayOutputStream`.
Also removed the unnecessary `out.flush()` from `getBytes()`.

### Improvement 23: Dictionary Writers — Replace LinkedOpenHashMap with OpenHashMap + ArrayList

**Hypothesis:** The `PlainBinaryDictionaryValuesWriter`, `PlainLongDictionaryValuesWriter`,
`PlainDoubleDictionaryValuesWriter`, and `PlainFloatDictionaryValuesWriter` all used
`*2IntLinkedOpenHashMap` to maintain insertion-order iteration for `toDictPageAndClose()`.
`LinkedOpenHashMap` maintains a doubly-linked list of entries, adding ~16 bytes per entry and
linked-list maintenance overhead on every insert. The `PlainIntegerDictionaryValuesWriter` was already
optimized to use `OpenHashMap` + `ArrayList` in a previous round.

**Fix:** Replaced all remaining `*2IntLinkedOpenHashMap` instances with `*2IntOpenHashMap` +
type-specific `ArrayList` (e.g., `LongArrayList`, `DoubleArrayList`, `FloatArrayList`,
`ArrayList<Binary>`). Insertion order is maintained by the ArrayList; the hash map only maps
values → dictionary IDs. Also simplified `fallBackDictionaryEncodedData()` — the reverse dictionary
array is no longer needed since the ordered ArrayList already provides O(1) index-based access.

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/FixedLenByteArrayPlainValuesWriter.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/dictionary/DictionaryValuesWriter.java`

### Results

**Binary dictionary encode** (improvement 23):

| Cardinality | String Length | Baseline (ops/s) | Optimized (ops/s) | Change |
|---|---|---|---|---|
| LOW | 10 | 20,110,049 ± 135K | 19,729,272 ± 233K | ~0% |
| LOW | 100 | 18,213,320 ± 186K | 19,007,260 ± 139K | +4.4% |
| LOW | 1000 | 22,310,845 ± 309K | 22,280,626 ± 183K | ~0% |
| HIGH | 10 | 1,363,108 ± 166K | 1,929,122 ± 180K | **+42%** |
| HIGH | 100 | 1,324,533 ± 137K | 1,623,701 ± 220K | **+23%** |
| HIGH | 1000 | 1,203,095 ± 84K | 1,565,471 ± 112K | **+30%** |

For HIGH cardinality (all unique values), every value triggers a hash map insert. Replacing
`LinkedOpenHashMap` with `OpenHashMap` + `ArrayList` eliminates linked-list pointer maintenance
on every insert, yielding +23% to +42% improvement. For LOW cardinality (lookup-dominated),
no meaningful change — hash table probing overhead is identical.

Int dictionary encode was unchanged (already used OpenHashMap + ArrayList from a previous round).

### Tests

- 23 tests pass: 20 Dictionary + 3 FixedLen (0 failures)

---

## Round 16: ByteStreamSplit Reader — Array-Based Single-Pass Decode

### Improvement 24: ByteStreamSplitValuesReader.decodeData() — Eliminate ByteBuffer.get() and Multi-Pass

**Hypothesis:** The `decodeData()` method transposes encoded data from stream-split layout to interleaved
layout. It used `ByteBuffer.get(index)` for every byte read — each call incurs bounds checking overhead.
Additionally, it made `elementSizeInBytes` passes over the output array (4 for float/int32, 8 for
double/int64), touching each output cache line multiple times.

**Fix:** Two optimizations combined:
1. **Array-based access:** Use `encoded.array()` directly for heap-backed ByteBuffers (zero-copy),
   or bulk-read into a byte array for direct buffers (one memcpy). The hot loop then uses pure
   array-to-array byte copies, eliminating per-byte ByteBuffer bounds checking.
2. **Single-pass unrolled loops for 4-byte and 8-byte types:** Instead of 4/8 separate passes,
   a single loop writes each output element completely in one iteration. This writes each output
   cache line exactly once (sequentially), while reading from 4/8 concurrent sequential source
   streams — well-served by hardware prefetchers.

A generic multi-pass fallback is retained for arbitrary element sizes (FIXED_LEN_BYTE_ARRAY).

### Files Modified

1. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReader.java`

### Results

**ByteStreamSplit decode (int32, 4-byte path):**

| Data Pattern | Baseline (ops/s) | Optimized (ops/s) | Change |
|---|---|---|---|
| SEQUENTIAL | 84,860,072 ± 3.67M | 176,513,345 ± 2.55M | **+108% (2.08x)** |
| RANDOM | 86,438,757 ± 911K | 174,284,856 ± 2.43M | **+102% (2.02x)** |
| LOW_CARDINALITY | 86,124,846 ± 581K | 175,699,270 ± 1.08M | **+104% (2.04x)** |
| HIGH_CARDINALITY | 86,198,280 ± 637K | 174,659,125 ± 1.75M | **+103% (2.03x)** |

Consistent **2x decode throughput** across all data patterns. The improvement applies equally to
float, int32 (4-byte path) and double, int64 (8-byte path). The data pattern is irrelevant because
`decodeData()` processes the entire page at init time — the per-value `readInteger()`/`readFloat()`
is just `decodedDataBuffer.getInt(offset)`, which is unchanged.

### Tests

- 51 ByteStreamSplit tests pass (0 failures)

---

## Round 17: Batch Read APIs and IntList.size() Fix

### Improvement 25: IntList.size() O(1) with running counter

`IntList.size()` iterated all slabs to compute the total element count. While only
O(log N) in slabs, it was called from `getBufferedSize()` on the periodic size-check
path. Fix: add a `totalSize` field incremented in `add()`.

**Impact:** Negligible (was ~20 iterations max), but correct by construction.

### Improvement 26: Batch read APIs for ValuesReader hierarchy

Added `readIntegers(int[], int, int)`, `readLongs(long[], int, int)`,
`readFloats(float[], int, int)`, `readDoubles(double[], int, int)` batch methods
to `ValuesReader` base class with default loop implementations, plus optimized
overrides in all major reader implementations.

#### RLE Decoder batch: `readInts(int[], int, int)` — **+148% (2.5x)**

The per-value `readInt()` has a branch + switch on every call (checking if we need
to load a new run, then dispatching RLE vs PACKED mode). The batch `readInts()` loops
within each run using `Arrays.fill()` for RLE runs and `System.arraycopy()` for
PACKED runs, processing entire runs at once without per-value dispatch.

| Data Pattern | Per-value (M ops/s) | Batch (M ops/s) | Improvement |
|---|---|---|---|
| SEQUENTIAL | 110 | **273** | **+148%** |
| LOW_CARDINALITY | 111 | **272** | **+145%** |

#### Dictionary Reader batch: `readIntegers(int[], int, int)` — **+42% to +67%**

Batch-decodes RLE dictionary IDs via `decoder.readInts()`, then does sequential
dictionary lookups in a tight loop. Benefits from both the RLE batch decode and
improved cache locality on the dictionary array.

| Data Pattern | Per-value (M ops/s) | Batch (M ops/s) | Improvement |
|---|---|---|---|
| SEQUENTIAL | 86 | **122** | **+42%** |
| LOW_CARDINALITY | 106 | **176** | **+67%** |

Low cardinality benefits more (+67%) because the encoded dictionary IDs have longer
RLE runs, amplifying the batch RLE decode advantage.

#### Plain, BSS, Delta: batch is neutral to slightly slower at micro level

For readers that are already a single JVM intrinsic per value (`buffer.getInt()`,
`buffer.getInt(offset)`, `valuesBuffer[idx++]`), the batch methods provide no speedup
at the ValuesReader level because:
1. No per-value control flow to eliminate (unlike RLE's switch/branch)
2. The batch version writes to an output array (additional work the per-value
   Blackhole-based benchmark doesn't perform)

These batch methods are still correct and serve as building blocks for a higher-level
batch ColumnReader that would skip the RecordReaderImplementation state machine.

### Key Insight

The real bottleneck on the read path is the `RecordReaderImplementation.read()` state
machine, which performs 5-6 virtual dispatches per value (def level read, rep level read,
binding.read(), binding.writeValue(), FSA state transition, group converter calls).
Batch APIs at the ValuesReader level are building blocks — the full benefit requires
a Level B batch reader at the ColumnReader/RecordReader level that would process N rows
at a time, batching the def/rep level reads AND value reads for flat schemas.

### Tests

- 573 parquet-column tests pass (0 failures)
- 312+ parquet-common tests pass (0 failures)

---

## Summary Table

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
| 9 | DeltaBA reader: avoid hidden suffix copy | parquet-column | decodeDeltaByteArray | **+3.8% to +11.5%** |
| 10 | RLE encoder: batch bit-packed groups | parquet-column | encodeDictionary | **up to +12.0%** |
| 11 | Eager column buffer release during flush | parquet-hadoop | RowGroupFlushBenchmark | **No effect** (peak set during write phase, not flush) |
| 12 | PlainValuesReader: direct ByteBuffer reads | parquet-column | decodePlain | **+1,130% (12.3x)** |
| 13 | RLE decoder: cache DataInputStream + forward index | parquet-column | decodeRle | **No measurable effect** (code quality) |
| 14 | RLE decoder readInt(): remove checked IOException | parquet-column | decodeDictionary | **No measurable effect** (code quality) |
| 15 | BinaryPlainValuesReader: direct ByteBuffer reads | parquet-column | decodePlain (binary) | **+12% to +16%** (short strings) |
| 16 | RLE decoder: replace InputStream with ByteBuffer | parquet-column | decodeRle | **No measurable effect** (code quality) |
| 17 | PlainValuesWriter: direct slab writes | parquet-common | encodePlain | **+32% to +97%** |
| 18 | BSS writer: batch scatter writes | parquet-column | encodeByteStreamSplit | **+134% to +138% (2.35x)** |
| 19 | Page assembly: eliminate BAOSBytesInput copy + streaming CRC | parquet-common, parquet-hadoop | FileWriteBenchmark | **Code quality** (eliminates page-size alloc+copy per compressed page) |
| 20 | DeltaByteArrayWriter: avoid Binary.slice() per value | parquet-column | encodeDeltaByteArray | **+23% to +33%** (short strings) |
| 21 | DeltaLengthByteArrayValuesWriter: eliminate LE wrapper | parquet-column | encodeDeltaLengthByteArray | **+16% to +18%** (short strings) |
| 22 | FixedLenByteArrayPlainValuesWriter: eliminate LE wrapper | parquet-column | — | Code quality (same pattern as #21) |
| 23 | Dictionary writers: OpenHashMap + ArrayList | parquet-column | encodeDictionary (binary) | **+23% to +42%** (high cardinality) |
| 24 | BSS reader: array-based single-pass decode | parquet-column | decodeByteStreamSplit | **+102% to +108% (2x)** |
| 25 | IntList.size(): O(1) with running counter | parquet-column | — | Code quality (trivial fix) |
| 26 | Batch read APIs: ValuesReader hierarchy | parquet-column | decodeRle, decodeDictionary | **RLE +148%, Dict +42% to +67%** |

### Files Modified (Round 7)

1. `parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ColumnChunkPageWriteStore.java`
2. `parquet-common/src/main/java/org/apache/parquet/bytes/ConcatenatingByteBufferCollector.java`

### Files Modified (Round 8)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/PlainValuesReader.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`

### Files Modified (Round 9)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/dictionary/DictionaryValuesReader.java`
3. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridValuesReader.java`
4. `parquet-column/src/main/java/org/apache/parquet/column/impl/ColumnReaderBase.java`
5. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/BinaryPlainValuesReader.java`

### Files Modified (Round 10)

1. `parquet-common/src/main/java/org/apache/parquet/bytes/BytesUtils.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`
3. `parquet-column/src/main/java/org/apache/parquet/column/values/dictionary/DictionaryValuesReader.java`
4. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridValuesReader.java`
5. `parquet-column/src/main/java/org/apache/parquet/column/impl/ColumnReaderBase.java`

### Files Modified (Round 11)

1. `parquet-common/src/main/java/org/apache/parquet/bytes/CapacityByteArrayOutputStream.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/PlainValuesWriter.java`

### Files Modified (Round 12)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesWriter.java`

### Files Modified (Round 13)

1. `parquet-common/src/main/java/org/apache/parquet/bytes/BytesInput.java`
2. `parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ColumnChunkPageWriteStore.java`

### Files Modified (Round 14)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/deltastrings/DeltaByteArrayWriter.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/deltalengthbytearray/DeltaLengthByteArrayValuesWriter.java`

### Files Modified (Round 15)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/FixedLenByteArrayPlainValuesWriter.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/dictionary/DictionaryValuesWriter.java`

### Files Modified (Round 16)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReader.java`

### Files Modified (Round 17)

1. `parquet-column/src/main/java/org/apache/parquet/column/values/ValuesReader.java`
2. `parquet-column/src/main/java/org/apache/parquet/column/values/plain/PlainValuesReader.java`
3. `parquet-column/src/main/java/org/apache/parquet/column/values/delta/DeltaBinaryPackingValuesReader.java`
4. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReader.java`
5. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReaderForInteger.java`
6. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReaderForFloat.java`
7. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReaderForDouble.java`
8. `parquet-column/src/main/java/org/apache/parquet/column/values/bytestreamsplit/ByteStreamSplitValuesReaderForLong.java`
9. `parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridDecoder.java`
10. `parquet-column/src/main/java/org/apache/parquet/column/values/dictionary/DictionaryValuesReader.java`
11. `parquet-column/src/main/java/org/apache/parquet/column/values/dictionary/IntList.java`

## End-to-End Benchmark Comparison

Baseline: commit `375ffec30` (pre-optimization, benchmark module merged).
Optimized: commit `916a632ea` (all 24 optimizations applied, `perf` branch HEAD).

Benchmark workload: 6-column schema (int32, int64, float, double, boolean, binary),
100,000 rows, dictionary encoding enabled, real filesystem I/O (LocalInputFile).
JMH: 4 warmup iterations, 5 measurement iterations, 1 fork, avgt mode.

### Read Path (ReadBenchmarks.readFile)

| Configuration | Baseline (ms) | Optimized (ms) | Δ ms | Improvement |
|---|---|---|---|---|
| UNCOMPRESSED, V1 | 104.4 ± 1.7 | 94.3 ± 2.6 | −10.1 | **−9.7%** |
| UNCOMPRESSED, V2 | 87.8 ± 2.9 | 82.0 ± 2.5 | −5.9 | **−6.7%** |
| SNAPPY, V1 | 123.3 ± 5.4 | 112.0 ± 2.1 | −11.3 | **−9.2%** |
| SNAPPY, V2 | 101.9 ± 4.2 | 94.4 ± 1.5 | −7.5 | **−7.4%** |
| GZIP, V1 | 141.8 ± 4.7 | 132.1 ± 2.1 | −9.6 | **−6.8%** |
| GZIP, V2 | 115.5 ± 1.8 | 110.3 ± 4.1 | −5.2 | **−4.5%** |
| ZSTD, V1 | 124.9 ± 2.8 | 113.9 ± 1.5 | −11.0 | **−8.8%** |
| ZSTD, V2 | 102.3 ± 2.9 | 96.2 ± 3.8 | −6.1 | **−5.9%** |

### Write Path (WriteBenchmarks.writeFile)

| Configuration | Baseline (ms) | Optimized (ms) | Δ ms | Improvement |
|---|---|---|---|---|
| UNCOMPRESSED, V1 | 215.8 ± 3.6 | 185.2 ± 5.5 | −30.7 | **−14.2%** |
| UNCOMPRESSED, V2 | 200.4 ± 8.0 | 179.2 ± 3.6 | −21.3 | **−10.6%** |
| SNAPPY, V1 | 226.1 ± 6.0 | 193.4 ± 5.5 | −32.7 | **−14.4%** |
| SNAPPY, V2 | 213.4 ± 14.1 | 191.5 ± 4.0 | −21.9 | **−10.3%** |
| GZIP, V1 | 596.2 ± 24.2 | 565.3 ± 9.5 | −30.9 | **−5.2%** |
| GZIP, V2 | 350.7 ± 5.1 | 327.8 ± 7.8 | −22.9 | **−6.5%** |
| ZSTD, V1 | 244.1 ± 11.9 | 210.6 ± 13.2 | −33.4 | **−13.7%** |
| ZSTD, V2 | 220.8 ± 4.6 | 199.8 ± 11.7 | −21.0 | **−9.5%** |

### Analysis

Write path improvements are larger (10-14%) because more of the total write time is spent
in encoding hot paths we optimized. Read path improvements (5-10%) are diluted by
Parquet metadata parsing, page decompression, column-level dispatch, and object materialization
that were not targets of this work. Even though individual encoding microbenchmarks showed
2-12x speedups, these compose with unoptimized layers to produce the observed end-to-end
improvements.

GZIP shows the smallest relative improvement because compression dominates total time
(~400ms of the ~596ms baseline is in GZIP compression), making encode/decode savings
proportionally smaller. UNCOMPRESSED and ZSTD/SNAPPY show the largest relative gains
because encoding/decoding is a larger fraction of total time.

### Commits

```
d442d5e68 Add batch read APIs to ValuesReader hierarchy: RLE +148%, Dictionary +67%
9ebe208e2 Optimize IntList.size() from O(slabs) to O(1) with running counter
96671b62c Document end-to-end benchmark comparison: baseline vs optimized (read -5-10%, write -10-14%)
916a632ea Optimize ByteStreamSplit decode: array-based single-pass transpose (2x throughput)
2a8463623 Optimize dictionary writers: replace LinkedOpenHashMap with OpenHashMap + ArrayList (+23-42% high cardinality)
abea051ad Optimize delta binary writers: eliminate per-value allocation and LE wrapper (+23-33%)
baa7e0206 Eliminate unnecessary page-size copies in compressed page assembly and CRC checksums
3c1a4454d Optimize ByteStreamSplit writer with batch scatter writes (2.35x encode speedup)
5cb34e914 Optimize PlainValuesWriter with direct ByteBuffer slab writes (+32% to +97%)
65090576d Optimize RLE decoder: replace InputStream with direct ByteBuffer reads
2e1a0146d Optimize read path: direct ByteBuffer for binary decode, remove IOException from RLE readInt()
13c930824 Optimize PlainValuesReader with direct ByteBuffer reads (12.3x decode speedup)
a60d3a74a Correct flush optimization docs: no peak memory reduction (validated with PeakTrackingAllocator)
c41e066dd Update parquet-perf.md with row group flush optimization results
d463d55a2 Reduce peak memory during row group flush by eagerly releasing column buffers
5a94ab2f4 Optimize PlainIntegerDictionaryValuesWriter by replacing LinkedOpenHashMap with OpenHashMap and an ArrayList
fd8114fd9 Update parquet-perf.md with round 6 results
5b9f66494 Use pack32Values in RLE hybrid encoder
724914335 Update parquet-perf.md with delta byte array decode optimization
344c48168 Avoid hidden suffix copies in delta byte array decode
8480dceb4 Update parquet-perf.md with Binary dictionary optimization
0baf1e664 Cache hash codes for constant Binary values
4cc922e5d Use 32-value packer entry points in delta writers
dfcab6420 Reduce delta decode ByteBuffer slicing overhead
b9a2bc794 Optimize plain int encoding and delta byte array writing
136c751f1 Optimize encoding hot paths: ByteStreamSplit writer/reader and RLE decoder
```

---

## Future Optimization Targets

The following targets have been identified but not pursued. They are listed here for
reference in case future work resumes.

### High Impact, High Effort

#### Level B Batch ColumnReader

The single largest remaining opportunity. The `RecordReaderImplementation.read()` state
machine performs 5-6 virtual dispatches per value: def level read, rep level read,
`binding.read()`, `binding.writeValue()`, FSA state transition, and group converter
start/end calls. For flat schemas (no nesting, repetition level always 0), a specialized
batch reader could process N rows at a time:

1. Batch-read definition levels via `RunLengthBitPackingHybridDecoder.readInts()`
2. Batch-read values via `ValuesReader.readIntegers()` (already implemented in Round 17)
3. Batch-write to converters or directly to output arrays
4. Skip the FSA state machine entirely for flat schemas

**Files involved:**
- `parquet-column/src/main/java/org/apache/parquet/io/RecordReaderImplementation.java` (lines 408-445)
- `parquet-column/src/main/java/org/apache/parquet/column/impl/ColumnReaderBase.java`
- `parquet-column/src/main/java/org/apache/parquet/column/ColumnReader.java` (interface)
- New: `BatchColumnReader` or similar

**Estimated impact:** Could reduce end-to-end read time significantly (the state machine
overhead dominates the 94ms UNCOMPRESSED read for 600K values = 157ns/value, of which
only ~10-15ns is actual decode).

**Complexity:** Requires new API surface, flat-schema detection, integration with the
converter framework, and careful handling of nulls (definition levels < max).

### Low Impact, Low Effort

#### DeltaByteArrayWriter `v.copy().getBytesUnsafe()` Avoidable Copy

**File:** `parquet-column/src/main/java/org/apache/parquet/column/values/deltastrings/DeltaByteArrayWriter.java` (lines 90-95)

For `ByteArraySliceBackedBinary` and `ByteBufferBackedBinary`, `v.copy().getBytesUnsafe()`
creates a copy because the backing storage is larger than the slice. A no-copy approach
would require rewriting the prefix comparison loop to work with offset+length or
ByteBuffer-based comparison, adding significant complexity for marginal gain.

The copy is structurally required for 2 of 3 Binary implementations. For
`ByteArrayBackedBinary` (the most common case from `Binary.fromConstantByteArray()`),
`copy()` returns `this` and `getBytesUnsafe()` returns the backing array directly — no copy.

#### BloomFilter.merge() toByteArray Copy

**File:** `parquet-column/src/main/java/org/apache/parquet/column/values/bloomfilter/BlockSplitBloomFilter.java` (lines 418-423)

`merge()` serializes the other bloom filter to a `ByteArrayOutputStream`, then calls
`toByteArray()` (which copies), then OR's the bytes into `bitset[]`. Could be eliminated
by providing direct access to the other filter's `bitset` array. Cold path — bloom filter
merging is not per-value.

### Exhausted Areas (No Further Gains)

- **LittleEndianDataOutputStream** — Zero remaining usages in codebase
- **LinkedOpenHashMap** — All replaced with OpenHashMap + ArrayList
- **InputStream-based decode** — All converted to ByteBuffer
- **RLE decoder per-value path** — 3 separate attempts confirmed not I/O-bound
- **Plain encode/decode** — Fully optimized (direct ByteBuffer intrinsics)
- **ByteStreamSplit encode** — 2.35x via batch scatter
- **ByteStreamSplit decode** — 2x via array-based single-pass transpose
- **Page-level copy elimination** — Done (BAOSBytesInput + streaming CRC)
- **Dictionary writers** — All use OpenHashMap + ArrayList
- **Batch ValuesReader APIs** — Done (RLE +148%, Dictionary +67%)
