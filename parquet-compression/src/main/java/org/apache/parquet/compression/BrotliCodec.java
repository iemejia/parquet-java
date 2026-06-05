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
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Brotli compression codec using brotli4j ({@code com.aayushatharva.brotli4j}) via reflection.
 */
final class BrotliCodec {

  private BrotliCodec() {}

  /**
   * Brotli compressor using brotli4j via reflection.
   * Single-call byte-array API — no streaming overhead. Default quality=1
   * matches the old jbrotli default and gives a good speed/ratio trade-off.
   */
  static final class Compressor extends DefaultCompressionCodecFactory.BytesCompressor {
    private final Object params;

    Compressor(int quality) {
      this.params = Brotli4j.newParams(quality);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      byte[] compressed = Brotli4j.compress(input, params);
      return BytesInput.from(compressed);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.BROTLI;
    }

    @Override
    public void release() {}
  }

  /**
   * Brotli decompressor using brotli4j via reflection.
   * Single-call byte-array API. For the ByteBuffer overload the input slice
   * is copied to a heap array, decompressed, and the result put into the
   * output buffer — Brotli is slow enough that the copy overhead is negligible.
   */
  static final class Decompressor extends DefaultCompressionCodecFactory.BytesDecompressor {

    Decompressor() {}

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      byte[] compressed = bytes.toByteArray();
      byte[] decompressed = Brotli4j.decompress(compressed);
      return BytesInput.from(decompressed);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      ByteBuffer inputSlice = input.slice();
      inputSlice.limit(compressedSize);
      byte[] compressedBytes = new byte[compressedSize];
      inputSlice.get(compressedBytes);

      byte[] decompressed = Brotli4j.decompress(compressedBytes);
      output.put(decompressed);
      input.position(input.position() + compressedSize);
    }

    @Override
    public void release() {}
  }
}
