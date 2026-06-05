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

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.Decoder;
import com.aayushatharva.brotli4j.encoder.Encoder;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Brotli compression codec using brotli4j ({@code com.aayushatharva.brotli4j}).
 * Single-call byte-array API for both compression and decompression.
 */
final class BrotliCodec {

  private BrotliCodec() {}

  /** Ensure brotli4j native library is loaded before first use. */
  static void ensureInitialized() {
    Brotli4jLoader.ensureAvailability();
  }

  static final class Compressor extends DefaultCompressionCodecFactory.BytesCompressor {
    private final Encoder.Parameters params;

    Compressor(int quality) {
      ensureInitialized();
      this.params = new Encoder.Parameters().setQuality(quality);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      byte[] compressed = Encoder.compress(input, params);
      return BytesInput.from(compressed);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.BROTLI;
    }

    @Override
    public void release() {}
  }

  static final class Decompressor extends DefaultCompressionCodecFactory.BytesDecompressor {

    Decompressor() {
      ensureInitialized();
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      byte[] compressed = bytes.toByteArray();
      byte[] decompressed = Decoder.decompress(compressed, 0, compressed.length);
      return BytesInput.from(decompressed);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      byte[] compressedBytes = new byte[compressedSize];
      input.slice().get(compressedBytes);

      byte[] decompressed = Decoder.decompress(compressedBytes, 0, compressedBytes.length);
      output.put(decompressed);
      input.position(input.position() + compressedSize);
    }

    @Override
    public void release() {}
  }
}
