/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.compression;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.Decoder;
import com.aayushatharva.brotli4j.encoder.Encoder;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.ReusingByteBufferAllocator;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.util.AutoCloseables;
import org.xerial.snappy.Snappy;

/**
 * Factory to produce compressors and decompressors that operate on java
 * direct memory, without requiring a copy into heap memory (where possible).
 */
class DirectCodecFactory extends DefaultCompressionCodecFactory implements AutoCloseable {

  private final ByteBufferAllocator allocator;

  /**
   * See docs on CodecFactory#createDirectCodecFactory which is how this class is
   * exposed publicly and is just a pass-through factory method for this constructor
   * to hide the rest of this class from public access.
   *
   * @throws NullPointerException if allocator is {@code null}
   */
  DirectCodecFactory(ParquetConfiguration config, ByteBufferAllocator allocator, int pageSize) {
    super(config, pageSize);

    this.allocator = java.util.Objects.requireNonNull(allocator, "allocator cannot be null");
    Preconditions.checkState(
        allocator.isDirect(),
        "A %s requires a direct buffer allocator be provided.",
        getClass().getSimpleName());
  }

  @Override
  protected BytesCompressor createCompressor(final CompressionCodecName codecName) {
    switch (codecName) {
      case SNAPPY:
        // avoid using the default Snappy codec since it allocates direct buffers at awkward spots.
        return new SnappyCompressor();
      case ZSTD:
        return new ZstdCompressor();
      case LZ4_RAW:
        return new Lz4RawCompressor();
      case BROTLI:
        return new BrotliDirectCompressor();
      case LZO:
      default:
        return super.createCompressor(codecName);
    }
  }

  @Override
  protected BytesDecompressor createDecompressor(final CompressionCodecName codecName) {
    switch (codecName) {
      case SNAPPY:
        return new SnappyDecompressor();
      case ZSTD:
        return new ZstdDecompressor();
      case LZ4_RAW:
        return new Lz4RawDecompressor();
      case BROTLI:
        return new BrotliDirectDecompressor();
      case GZIP:
      case LZO:
      case UNCOMPRESSED:
        return super.createDecompressor(codecName);
      default:
        return super.createDecompressor(codecName);
    }
  }

  public void close() {
    release();
  }

  private abstract class BaseDecompressor extends BytesDecompressor {
    private final ReusingByteBufferAllocator inputAllocator;
    private final ReusingByteBufferAllocator outputAllocator;

    BaseDecompressor() {
      inputAllocator = ReusingByteBufferAllocator.strict(allocator);
      // Using unsafe reusing allocator because we give out the output ByteBuffer wrapped in a BytesInput. But
      // that's what BytesInputs are for. It is expected to copy the data from the returned BytesInput before
      // using this decompressor again.
      outputAllocator = ReusingByteBufferAllocator.unsafe(allocator);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      try (ByteBufferReleaser releaser = inputAllocator.getReleaser()) {
        ByteBuffer input = bytes.toByteBuffer(releaser);
        ByteBuffer output = outputAllocator.allocate(decompressedSize);
        int size = decompress(input.slice(), output.slice());
        if (size != decompressedSize) {
          throw new IOException("Unexpected decompressed size: " + size + " != " + decompressedSize);
        }
        output.limit(size);
        return BytesInput.from(output);
      }
    }

    abstract int decompress(ByteBuffer input, ByteBuffer output) throws IOException;

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      int origInputLimit = input.limit();
      input.limit(input.position() + compressedSize);
      int origOutputLimit = output.limit();
      output.limit(output.position() + decompressedSize);
      int size = decompress(input.slice(), output.slice());
      if (size != decompressedSize) {
        throw new IOException("Unexpected decompressed size: " + size + " != " + decompressedSize);
      }
      input.position(input.limit());
      input.limit(origInputLimit);
      output.position(output.limit());
      output.limit(origOutputLimit);
    }

    @Override
    public void release() {
      AutoCloseables.uncheckedClose(outputAllocator, inputAllocator, this::closeDecompressor);
    }

    abstract void closeDecompressor();
  }

  private abstract class BaseCompressor extends BytesCompressor {
    private final ReusingByteBufferAllocator inputAllocator;
    private final ReusingByteBufferAllocator outputAllocator;

    BaseCompressor() {
      inputAllocator = ReusingByteBufferAllocator.strict(allocator);
      // Using unsafe reusing allocator because we give out the output ByteBuffer wrapped in a BytesInput. But
      // that's what BytesInputs are for. It is expected to copy the data from the returned BytesInput before
      // using this compressor again.
      outputAllocator = ReusingByteBufferAllocator.unsafe(allocator);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      try (ByteBufferReleaser releaser = inputAllocator.getReleaser()) {
        ByteBuffer input = bytes.toByteBuffer(releaser);
        ByteBuffer output = outputAllocator.allocate(maxCompressedSize(Math.toIntExact(bytes.size())));
        int size = compress(input.slice(), output.slice());
        output.limit(size);
        return BytesInput.from(output);
      }
    }

    abstract int maxCompressedSize(int size);

    abstract int compress(ByteBuffer input, ByteBuffer output) throws IOException;

    @Override
    public void release() {
      AutoCloseables.uncheckedClose(outputAllocator, inputAllocator, this::closeCompressor);
    }

    abstract void closeCompressor();
  }

  /**
   * @deprecated Use {@link DefaultCompressionCodecFactory#NO_OP_DECOMPRESSOR} instead
   */
  @Deprecated
  public class NoopDecompressor extends BytesDecompressor {

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      NO_OP_DECOMPRESSOR.decompress(input, compressedSize, output, decompressedSize);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      return NO_OP_DECOMPRESSOR.decompress(bytes, decompressedSize);
    }

    @Override
    public void release() {
      NO_OP_DECOMPRESSOR.release();
    }
  }

  public class SnappyDecompressor extends BaseDecompressor {
    @Override
    int decompress(ByteBuffer input, ByteBuffer output) throws IOException {
      return Snappy.uncompress(input, output);
    }

    @Override
    void closeDecompressor() {
      // no-op
    }
  }

  public class SnappyCompressor extends BaseCompressor {

    @Override
    int compress(ByteBuffer input, ByteBuffer output) throws IOException {
      return Snappy.compress(input, output);
    }

    @Override
    int maxCompressedSize(int size) {
      return Snappy.maxCompressedLength(size);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.SNAPPY;
    }

    @Override
    void closeCompressor() {
      // no-op
    }
  }

  private class ZstdDecompressor extends BaseDecompressor {
    private final ZstdDecompressCtx context;

    ZstdDecompressor() {
      context = new ZstdDecompressCtx();
    }

    @Override
    int decompress(ByteBuffer input, ByteBuffer output) {
      return context.decompress(output, input);
    }

    @Override
    void closeDecompressor() {
      context.close();
    }
  }

  /**
   * Direct-memory LZ4_RAW decompressor using airlift's LZ4 decompressor with
   * direct ByteBuffers.
   */
  private class Lz4RawDecompressor extends BaseDecompressor {
    private final io.airlift.compress.lz4.Lz4Decompressor decompressor =
        new io.airlift.compress.lz4.Lz4Decompressor();

    @Override
    int decompress(ByteBuffer input, ByteBuffer output) {
      decompressor.decompress(input, output);
      return output.position();
    }

    @Override
    void closeDecompressor() {
      // no-op
    }
  }

  private class ZstdCompressor extends BaseCompressor {
    private final ZstdCompressCtx context;

    ZstdCompressor() {
      context = new ZstdCompressCtx();
      context.setLevel(conf.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL));
      context.setWorkers(conf.getInt(PARQUET_COMPRESS_ZSTD_WORKERS, DEFAULT_PARQUET_COMPRESS_ZSTD_WORKERS));
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.ZSTD;
    }

    @Override
    int maxCompressedSize(int size) {
      return Math.toIntExact(Zstd.compressBound(size));
    }

    @Override
    int compress(ByteBuffer input, ByteBuffer output) {
      return context.compress(output, input);
    }

    @Override
    void closeCompressor() {
      context.close();
    }
  }

  /**
   * Direct-memory LZ4_RAW compressor using airlift's LZ4 compressor with
   * direct ByteBuffers, avoiding the stream-based heap path.
   */
  private class Lz4RawCompressor extends BaseCompressor {
    private final io.airlift.compress.lz4.Lz4Compressor compressor = new io.airlift.compress.lz4.Lz4Compressor();

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.LZ4_RAW;
    }

    @Override
    int maxCompressedSize(int size) {
      return compressor.maxCompressedLength(size);
    }

    @Override
    int compress(ByteBuffer input, ByteBuffer output) {
      compressor.compress(input, output);
      return output.position();
    }

    @Override
    void closeCompressor() {
      // no-op
    }
  }

  /**
   * Direct-memory Brotli decompressor using brotli4j via reflection.
   * brotli4j only exposes a byte-array API, so input/output are copied through heap arrays.
   * Brotli is slow enough that the copy overhead is negligible.
   */
  private class BrotliDirectDecompressor extends BaseDecompressor {

    BrotliDirectDecompressor() {
      Brotli4jLoader.ensureAvailability();
    }

    @Override
    int decompress(ByteBuffer input, ByteBuffer output) throws IOException {
      byte[] compressedBytes = new byte[input.remaining()];
      input.get(compressedBytes);
      byte[] decompressed = Decoder.decompress(compressedBytes, 0, compressedBytes.length);
      output.put(decompressed);
      return decompressed.length;
    }

    @Override
    void closeDecompressor() {
      // no-op
    }
  }

  /**
   * Direct-memory Brotli compressor using brotli4j.
   * Uses quality=1 by default (fast compression, matching the old jbrotli default).
   * brotli4j only exposes a byte-array API, so input/output are copied through heap arrays.
   */
  private class BrotliDirectCompressor extends BaseCompressor {
    private final Encoder.Parameters params;

    BrotliDirectCompressor() {
      Brotli4jLoader.ensureAvailability();
      this.params = new Encoder.Parameters().setQuality(1);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.BROTLI;
    }

    @Override
    int maxCompressedSize(int size) {
      // Brotli worst case: input size + (input size >> 2) + 1K overhead for small inputs
      return size + (size >> 2) + 1024;
    }

    @Override
    int compress(ByteBuffer input, ByteBuffer output) throws IOException {
      byte[] inputBytes = new byte[input.remaining()];
      input.get(inputBytes);
      byte[] compressed = Encoder.compress(inputBytes, params);
      output.put(compressed);
      return compressed.length;
    }

    @Override
    void closeCompressor() {
      // no-op
    }
  }

  /**
   * @deprecated Use {@link DefaultCompressionCodecFactory#NO_OP_COMPRESSOR} instead
   */
  @Deprecated
  public static class NoopCompressor extends BytesCompressor {

    public NoopCompressor() {}

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      return NO_OP_COMPRESSOR.compress(bytes);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return NO_OP_COMPRESSOR.getCodecName();
    }

    @Override
    public void release() {
      NO_OP_COMPRESSOR.release();
    }
  }
}
