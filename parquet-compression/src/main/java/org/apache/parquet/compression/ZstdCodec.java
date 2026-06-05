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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * ZSTD compression codec using zstd-jni context API directly.
 */
final class ZstdCodec {

  private ZstdCodec() {}

  /**
   * Compresses using a reusable {@link ZstdCompressCtx}. The context is created once
   * at construction and reused across calls, avoiding per-call JNI context creation,
   * internal buffer allocation, and Java stream overhead. Multi-threaded compression
   * via {@code workers > 0} is supported through {@link ZstdCompressCtx#setWorkers(int)}.
   */
  static final class Compressor extends DefaultCompressionCodecFactory.BytesCompressor {
    private final ZstdCompressCtx context;
    private byte[] outputBuffer;

    Compressor(int level, int workers) {
      this.context = new ZstdCompressCtx();
      this.context.setLevel(level);
      if (workers > 0) {
        this.context.setWorkers(workers);
      }
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      int maxLen = (int) Zstd.compressBound(input.length);
      if (outputBuffer == null || outputBuffer.length < maxLen) {
        outputBuffer = new byte[maxLen];
      }
      int compressed = context.compressByteArray(outputBuffer, 0, outputBuffer.length, input, 0, input.length);
      return BytesInput.from(outputBuffer, 0, compressed);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.ZSTD;
    }

    @Override
    public void release() {
      context.close();
      outputBuffer = null;
    }
  }

  /**
   * Decompresses using a reusable {@link ZstdDecompressCtx}, bypassing the Hadoop
   * codec framework. The context is created once at construction and reused across
   * calls, avoiding per-call JNI context creation, internal buffer allocation, and
   * Java stream overhead. The {@link ByteBuffer} overload uses
   * {@link Zstd#decompress(ByteBuffer, ByteBuffer)} to pass buffers directly to the
   * native library without intermediate copies.
   */
  static final class Decompressor extends DefaultCompressionCodecFactory.BytesDecompressor {
    private final ZstdDecompressCtx context;

    Decompressor() {
      this.context = new ZstdDecompressCtx();
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      byte[] input = bytes.toByteArray();
      byte[] output = new byte[decompressedSize];
      int decompressed = context.decompressByteArray(output, 0, decompressedSize, input, 0, input.length);
      if (decompressed != decompressedSize) {
        throw new IOException("Unexpected decompressed size: " + decompressed + " != " + decompressedSize);
      }
      return BytesInput.from(output);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      int origInputLimit = input.limit();
      input.limit(input.position() + compressedSize);
      int origOutputLimit = output.limit();
      output.limit(output.position() + decompressedSize);
      // Zstd.decompress uses (dst, src) parameter order, matching the native zstd convention.
      // Use slices so native API works on independent buffers; advance positions manually.
      Zstd.decompress(output.slice(), input.slice());
      input.position(input.limit());
      input.limit(origInputLimit);
      output.position(output.limit());
      output.limit(origOutputLimit);
    }

    @Override
    public void release() {
      context.close();
    }
  }
}
