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

import io.airlift.compress.lzo.LzoHadoopStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * LZO compression codec using aircompressor's Hadoop-framed LZO directly.
 */
final class LzoCodec {

  private LzoCodec() {}

  /**
   * Compresses using aircompressor's LZO Hadoop-framed streams directly,
   * bypassing the GPL-licensed {@code com.hadoop.compression.lzo.LzoCodec} and
   * the associated Hadoop codec pool / stream wrapper overhead. The framing
   * format (big-endian length-prefixed blocks) is wire-compatible with Hadoop's
   * LzoCodec, so files produced by this compressor are readable by any standard
   * Parquet reader.
   */
  static final class Compressor extends DefaultCompressionCodecFactory.BytesCompressor {
    private static final LzoHadoopStreams LZO_STREAMS = new LzoHadoopStreams();
    private final java.io.ByteArrayOutputStream baos;

    Compressor(int pageSize) {
      this.baos = new java.io.ByteArrayOutputStream(pageSize);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      baos.reset();
      try (OutputStream los = LZO_STREAMS.createOutputStream(baos)) {
        bytes.writeAllTo(los);
      }
      return BytesInput.from(baos);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.LZO;
    }

    @Override
    public void release() {}
  }

  /**
   * Decompresses using aircompressor's LZO Hadoop-framed streams directly,
   * bypassing the GPL-licensed Hadoop LzoCodec. Reads the same big-endian
   * length-prefixed block framing that Hadoop's LzoCodec produces.
   */
  static final class Decompressor extends DefaultCompressionCodecFactory.BytesDecompressor {
    private static final LzoHadoopStreams LZO_STREAMS = new LzoHadoopStreams();

    Decompressor() {}

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      try (InputStream lis = LZO_STREAMS.createInputStream(bytes.toInputStream())) {
        byte[] output = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = lis.read(output, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of LZO stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        return BytesInput.from(output);
      }
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      ByteBuffer inputSlice = input.slice();
      inputSlice.limit(compressedSize);
      try (InputStream lis = LZO_STREAMS.createInputStream(ByteBufferInputStream.wrap(inputSlice))) {
        byte[] outputBytes = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = lis.read(outputBytes, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of LZO stream at offset " + offset + " of " + decompressedSize);
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
