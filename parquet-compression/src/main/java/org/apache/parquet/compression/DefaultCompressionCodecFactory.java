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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class DefaultCompressionCodecFactory implements CompressionCodecFactory {

  // ZSTD configuration constants (previously in ZstandardCodec)
  public static final String PARQUET_COMPRESS_ZSTD_LEVEL = "parquet.compression.codec.zstd.level";
  public static final int DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL = 3;
  public static final String PARQUET_COMPRESS_ZSTD_WORKERS = "parquet.compression.codec.zstd.workers";
  public static final int DEFAULT_PARQUET_COMPRESS_ZSTD_WORKERS = 0;

  private final Map<CompressionCodecName, BytesCompressor> compressors = new HashMap<>();
  private final Map<CompressionCodecName, BytesDecompressor> decompressors = new HashMap<>();

  protected final ParquetConfiguration conf;
  protected final int pageSize;

  public static final BytesDecompressor NO_OP_DECOMPRESSOR = new BytesDecompressor() {
    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) {
      Preconditions.checkArgument(
          compressedSize == decompressedSize,
          "Non-compressed data did not have matching compressed and decompressed sizes.");
      Preconditions.checkArgument(
          input.remaining() >= compressedSize, "Not enough bytes available in the input buffer");
      int origLimit = input.limit();
      input.limit(input.position() + compressedSize);
      output.put(input);
      input.limit(origLimit);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) {
      return bytes;
    }

    @Override
    public void release() {}
  };

  public static final BytesCompressor NO_OP_COMPRESSOR = new BytesCompressor() {
    @Override
    public BytesInput compress(BytesInput bytes) {
      return bytes;
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.UNCOMPRESSED;
    }

    @Override
    public void release() {}
  };

  /**
   * Create a new codec factory.
   *
   * @param configuration used to pass compression codec configuration information
   * @param pageSize      the expected page size, does not set a hard limit, currently just
   *                      used to set the initial size of the output stream used when
   *                      compressing a buffer. If this factory is only used to construct
   *                      decompressors this parameter has no impact on the function of the factory
   */
  public DefaultCompressionCodecFactory(ParquetConfiguration configuration, int pageSize) {
    this.conf = configuration;
    this.pageSize = pageSize;
  }

  /**
   * Create a codec factory that will provide compressors and decompressors
   * that will work natively with ByteBuffers backed by direct memory.
   *
   * @param config    configuration options for different compression codecs
   * @param allocator an allocator for creating result buffers during compression
   *                  and decompression, must provide buffers backed by Direct memory
   * @param pageSize  the default page size
   * @return a configured direct codec factory
   */
  public static DefaultCompressionCodecFactory createDirectCodecFactory(
      ParquetConfiguration config, ByteBufferAllocator allocator, int pageSize) {
    return new DirectCodecFactory(config, allocator, pageSize);
  }

  @Override
  public BytesCompressor getCompressor(CompressionCodecName codecName) {
    BytesCompressor comp = compressors.get(codecName);
    if (comp == null) {
      comp = createCompressor(codecName);
      compressors.put(codecName, comp);
    }
    return comp;
  }

  @Override
  public BytesDecompressor getDecompressor(CompressionCodecName codecName) {
    BytesDecompressor decomp = decompressors.get(codecName);
    if (decomp == null) {
      decomp = createDecompressor(codecName);
      decompressors.put(codecName, decomp);
    }
    return decomp;
  }

  protected BytesCompressor createCompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return NO_OP_COMPRESSOR;
      case SNAPPY:
        return new SnappyCodec.Compressor();
      case ZSTD:
        return new ZstdCodec.Compressor(
            conf.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL),
            conf.getInt(PARQUET_COMPRESS_ZSTD_WORKERS, DEFAULT_PARQUET_COMPRESS_ZSTD_WORKERS));
      case LZ4_RAW:
        return new Lz4RawCodec.Compressor();
      case GZIP:
        int gzipLevel = conf.getInt("zlib.compress.level", Deflater.DEFAULT_COMPRESSION);
        return new GzipCodec.Compressor(gzipLevel, pageSize);
      case LZO:
        return new LzoCodec.Compressor(pageSize);
      case BROTLI:
        int brotliQuality = conf.getInt("compression.brotli.quality", 1);
        return new BrotliCodec.Compressor(brotliQuality);
      default:
        throw new UnsupportedOperationException("Codec not supported: " + codecName);
    }
  }

  protected BytesDecompressor createDecompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return NO_OP_DECOMPRESSOR;
      case SNAPPY:
        return new SnappyCodec.Decompressor();
      case ZSTD:
        return new ZstdCodec.Decompressor();
      case LZ4_RAW:
        return new Lz4RawCodec.Decompressor();
      case GZIP:
        return new GzipCodec.Decompressor();
      case LZO:
        return new LzoCodec.Decompressor();
      case BROTLI:
        return new BrotliCodec.Decompressor();
      default:
        throw new UnsupportedOperationException("Codec not supported: " + codecName);
    }
  }

  @Override
  public void release() {
    for (BytesCompressor compressor : compressors.values()) {
      compressor.release();
    }
    compressors.clear();
    for (BytesDecompressor decompressor : decompressors.values()) {
      decompressor.release();
    }
    decompressors.clear();
  }

  /**
   * @deprecated will be removed in 2.0.0; use CompressionCodecFactory.BytesInputCompressor instead.
   */
  @Deprecated
  public abstract static class BytesCompressor implements CompressionCodecFactory.BytesInputCompressor {
    public abstract BytesInput compress(BytesInput bytes) throws IOException;

    public abstract CompressionCodecName getCodecName();

    public abstract void release();
  }

  /**
   * @deprecated will be removed in 2.0.0; use CompressionCodecFactory.BytesInputDecompressor instead.
   */
  @Deprecated
  public abstract static class BytesDecompressor implements CompressionCodecFactory.BytesInputDecompressor {
    public abstract BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException;

    public abstract void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException;

    public abstract void release();
  }
}
