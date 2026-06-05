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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * GZIP compression codec using JDK GZIPOutputStream/GZIPInputStream directly.
 */
final class GzipCodec {

  private GzipCodec() {}

  /**
   * Compresses using {@link GZIPOutputStream} directly, bypassing Hadoop's
   * GzipCodec and the associated codec pool / stream wrapper overhead.
   *
   * <p>Note: this implementation always uses Java's built-in zlib via
   * {@link GZIPOutputStream}. It does <em>not</em> use Hadoop native libraries,
   * so hardware-accelerated compression via Intel ISA-L will not be used even if
   * the native libraries are installed. The overhead reduction from bypassing the
   * Hadoop codec framework typically outweighs the ISA-L advantage for the page
   * sizes used by Parquet.
   */
  static final class Compressor extends DefaultCompressionCodecFactory.BytesCompressor {
    private final int level;
    private final ByteArrayOutputStream baos;

    Compressor(int level, int pageSize) {
      this.level = level;
      this.baos = new ByteArrayOutputStream(pageSize);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      baos.reset();
      try (GZIPOutputStream gos = new GZIPOutputStream(baos) {
        {
          def.setLevel(level);
        }
      }) {
        bytes.writeAllTo(gos);
      }
      return BytesInput.from(baos);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.GZIP;
    }

    @Override
    public void release() {}
  }

  /**
   * Decompresses using {@link GZIPInputStream} directly, bypassing Hadoop's
   * GzipCodec and the associated codec pool / stream wrapper overhead.
   * CRC32 and size verification is handled by the JDK implementation.
   *
   * <p>Note: this implementation always uses Java's built-in zlib via
   * {@link GZIPInputStream}. It does <em>not</em> use Hadoop native libraries,
   * so hardware-accelerated decompression via Intel ISA-L will not be used even if
   * the native libraries are installed.
   */
  static final class Decompressor extends DefaultCompressionCodecFactory.BytesDecompressor {

    Decompressor() {}

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      try (GZIPInputStream gis = new GZIPInputStream(bytes.toInputStream())) {
        byte[] output = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = gis.read(output, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of GZIP stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        return BytesInput.from(output);
      }
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      // Wrap the input ByteBuffer slice in an InputStream to avoid allocating a temp byte array.
      // GZIPInputStream is stream-based so we still need a temp output array.
      ByteBuffer inputSlice = input.slice();
      inputSlice.limit(compressedSize);
      try (GZIPInputStream gis = new GZIPInputStream(ByteBufferInputStream.wrap(inputSlice))) {
        byte[] outputBytes = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = gis.read(outputBytes, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of GZIP stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        output.put(outputBytes);
      }
      input.position(input.position() + compressedSize);
    }

    @Override
    public void release() {}
  }
}
