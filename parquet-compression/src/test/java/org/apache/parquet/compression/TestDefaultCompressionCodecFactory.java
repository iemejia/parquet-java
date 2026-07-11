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
package org.apache.parquet.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;

/**
 * Unit tests for {@link DefaultCompressionCodecFactory}.
 *
 * <p>Tests each codec implementation directly (compress/decompress round-trip)
 * without any Hadoop or Parquet file I/O dependencies.
 */
public class TestDefaultCompressionCodecFactory {

  private static final int PAGE_SIZE = 64 * 1024;

  private static final CompressionCodecName[] ALL_CODECS = {
    CompressionCodecName.SNAPPY,
    CompressionCodecName.GZIP,
    CompressionCodecName.ZSTD,
    CompressionCodecName.LZ4_RAW,
    CompressionCodecName.LZO,
    CompressionCodecName.BROTLI,
  };

  // ---- Round-trip tests for each codec ----

  @Test
  public void roundTripSnappy() throws IOException {
    verifyRoundTrip(CompressionCodecName.SNAPPY);
  }

  @Test
  public void roundTripGzip() throws IOException {
    verifyRoundTrip(CompressionCodecName.GZIP);
  }

  @Test
  public void roundTripZstd() throws IOException {
    verifyRoundTrip(CompressionCodecName.ZSTD);
  }

  @Test
  public void roundTripLz4Raw() throws IOException {
    verifyRoundTrip(CompressionCodecName.LZ4_RAW);
  }

  @Test
  public void roundTripLzo() throws IOException {
    verifyRoundTrip(CompressionCodecName.LZO);
  }

  @Test
  public void roundTripBrotli() throws IOException {
    verifyRoundTrip(CompressionCodecName.BROTLI);
  }

  // ---- All codecs with multiple data sizes ----

  @Test
  public void roundTripAllCodecsMultipleSizes() throws IOException {
    int[] sizes = {0, 1, 10, 1024, 64 * 1024, 256 * 1024};
    for (CompressionCodecName codec : ALL_CODECS) {
      for (int size : sizes) {
        verifyRoundTrip(codec, size, "codec=" + codec + " size=" + size);
      }
    }
  }

  // ---- ZSTD configuration tests ----

  @Test
  public void zstdLevelAffectsCompression() throws IOException {
    PlainParquetConfiguration confLow = new PlainParquetConfiguration();
    confLow.setInt(DefaultCompressionCodecFactory.PARQUET_COMPRESS_ZSTD_LEVEL, 1);

    PlainParquetConfiguration confHigh = new PlainParquetConfiguration();
    confHigh.setInt(DefaultCompressionCodecFactory.PARQUET_COMPRESS_ZSTD_LEVEL, 19);

    byte[] data = compressibleData(64 * 1024);

    DefaultCompressionCodecFactory factoryLow = new DefaultCompressionCodecFactory(confLow, PAGE_SIZE);
    DefaultCompressionCodecFactory factoryHigh = new DefaultCompressionCodecFactory(confHigh, PAGE_SIZE);

    long sizeLow = factoryLow
        .getCompressor(CompressionCodecName.ZSTD)
        .compress(BytesInput.from(data))
        .size();
    long sizeHigh = factoryHigh
        .getCompressor(CompressionCodecName.ZSTD)
        .compress(BytesInput.from(data))
        .size();

    // Higher level should produce smaller output
    assertTrue(
        "ZSTD level=19 should compress better than level=1, got " + sizeHigh + " >= " + sizeLow,
        sizeHigh < sizeLow);

    factoryLow.release();
    factoryHigh.release();
  }

  @Test
  public void zstdWorkersConfigRoundTrip() throws IOException {
    PlainParquetConfiguration conf = new PlainParquetConfiguration();
    conf.setInt(DefaultCompressionCodecFactory.PARQUET_COMPRESS_ZSTD_WORKERS, 2);

    DefaultCompressionCodecFactory factory = new DefaultCompressionCodecFactory(conf, PAGE_SIZE);
    byte[] data = compressibleData(64 * 1024);

    CompressionCodecFactory.BytesInputCompressor compressor = factory.getCompressor(CompressionCodecName.ZSTD);
    CompressionCodecFactory.BytesInputDecompressor decompressor =
        factory.getDecompressor(CompressionCodecName.ZSTD);

    BytesInput compressed = compressor.compress(BytesInput.from(data));
    BytesInput decompressed = decompressor.decompress(compressed, data.length);
    assertArrayEquals("ZSTD with workers=2 round-trip failed", data, decompressed.toByteArray());

    factory.release();
  }

  // ---- GZIP level test ----

  @Test
  public void gzipLevelAffectsCompression() throws IOException {
    PlainParquetConfiguration confFast = new PlainParquetConfiguration();
    confFast.setInt("zlib.compress.level", 1);

    PlainParquetConfiguration confBest = new PlainParquetConfiguration();
    confBest.setInt("zlib.compress.level", 9);

    byte[] data = compressibleData(64 * 1024);

    DefaultCompressionCodecFactory factoryFast = new DefaultCompressionCodecFactory(confFast, PAGE_SIZE);
    DefaultCompressionCodecFactory factoryBest = new DefaultCompressionCodecFactory(confBest, PAGE_SIZE);

    long sizeFast = factoryFast
        .getCompressor(CompressionCodecName.GZIP)
        .compress(BytesInput.from(data))
        .size();
    long sizeBest = factoryBest
        .getCompressor(CompressionCodecName.GZIP)
        .compress(BytesInput.from(data))
        .size();

    assertTrue(
        "GZIP level=9 should compress better than level=1, got " + sizeBest + " >= " + sizeFast,
        sizeBest < sizeFast);

    factoryFast.release();
    factoryBest.release();
  }

  // ---- Compressor reuse safety ----

  @Test
  public void consecutiveCompressionsProduceCorrectResults() throws IOException {
    DefaultCompressionCodecFactory factory = createFactory();

    for (CompressionCodecName codec : ALL_CODECS) {
      CompressionCodecFactory.BytesInputCompressor compressor = factory.getCompressor(codec);
      CompressionCodecFactory.BytesInputDecompressor decompressor = factory.getDecompressor(codec);

      for (int i = 0; i < 5; i++) {
        byte[] data = compressibleData(1024 + i * 512);
        BytesInput compressed = compressor.compress(BytesInput.from(data));
        BytesInput decompressed = decompressor.decompress(compressed, data.length);
        assertArrayEquals(codec + " iteration " + i + " failed", data, decompressed.toByteArray());
      }
    }

    factory.release();
  }

  // ---- Uncompressed pass-through ----

  @Test
  public void uncompressedPassThrough() throws IOException {
    DefaultCompressionCodecFactory factory = createFactory();
    byte[] data = compressibleData(1024);

    CompressionCodecFactory.BytesInputCompressor compressor =
        factory.getCompressor(CompressionCodecName.UNCOMPRESSED);
    CompressionCodecFactory.BytesInputDecompressor decompressor =
        factory.getDecompressor(CompressionCodecName.UNCOMPRESSED);

    BytesInput compressed = compressor.compress(BytesInput.from(data));
    assertEquals("UNCOMPRESSED should not change size", data.length, compressed.size());

    BytesInput decompressed = decompressor.decompress(compressed, data.length);
    assertArrayEquals(data, decompressed.toByteArray());

    factory.release();
  }

  // ---- Helpers ----

  private void verifyRoundTrip(CompressionCodecName codec) throws IOException {
    verifyRoundTrip(codec, 64 * 1024, codec.name());
  }

  private void verifyRoundTrip(CompressionCodecName codec, int dataSize, String label) throws IOException {
    DefaultCompressionCodecFactory factory = createFactory();
    byte[] data = compressibleData(dataSize);

    CompressionCodecFactory.BytesInputCompressor compressor = factory.getCompressor(codec);
    CompressionCodecFactory.BytesInputDecompressor decompressor = factory.getDecompressor(codec);

    BytesInput compressed = compressor.compress(BytesInput.from(data));

    BytesInput decompressed = decompressor.decompress(compressed, data.length);
    assertArrayEquals(label + ": round-trip failed", data, decompressed.toByteArray());

    factory.release();
  }

  private DefaultCompressionCodecFactory createFactory() {
    return new DefaultCompressionCodecFactory(new PlainParquetConfiguration(), PAGE_SIZE);
  }

  /** Generate data with repeating patterns that compresses well. */
  private byte[] compressibleData(int size) {
    byte[] data = new byte[size];
    Random rng = new Random(42);
    // Fill with repeating patterns - highly compressible
    for (int i = 0; i < size; i++) {
      data[i] = (byte) (rng.nextInt(10) + 'A');
    }
    return data;
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    new Random(42).nextBytes(data);
    return data;
  }
}
