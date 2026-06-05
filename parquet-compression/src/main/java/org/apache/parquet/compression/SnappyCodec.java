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
import org.xerial.snappy.Snappy;

/**
 * Snappy compression codec using direct JNI calls, bypassing the Hadoop
 * stream abstraction.
 */
final class SnappyCodec {

  private SnappyCodec() {}

  /**
   * Compresses using Snappy's byte-array JNI API directly, bypassing the Hadoop
   * stream abstraction. This avoids intermediate direct ByteBuffer copies and
   * reduces the compression to a single native call per page.
   */
  static final class Compressor extends DefaultCompressionCodecFactory.BytesCompressor {
    private byte[] outputBuffer;

    Compressor() {}

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      int maxLen = Snappy.maxCompressedLength(input.length);
      if (outputBuffer == null || outputBuffer.length < maxLen) {
        outputBuffer = new byte[maxLen];
      }
      int compressed = Snappy.compress(input, 0, input.length, outputBuffer, 0);
      return BytesInput.from(outputBuffer, 0, compressed);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.SNAPPY;
    }

    @Override
    public void release() {
      outputBuffer = null;
    }
  }

  /**
   * Decompresses using Snappy's JNI API directly. The {@link ByteBuffer} overload uses
   * {@link Snappy#uncompress(ByteBuffer, ByteBuffer)} which, for direct buffers, passes
   * native memory addresses straight to the snappy library with no JNI array pinning or
   * intermediate copies.
   */
  static final class Decompressor extends DefaultCompressionCodecFactory.BytesDecompressor {

    Decompressor() {}

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      byte[] input = bytes.toByteArray();
      byte[] output = new byte[decompressedSize];
      Snappy.uncompress(input, 0, input.length, output, 0);
      return BytesInput.from(output);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      int origInputLimit = input.limit();
      input.limit(input.position() + compressedSize);
      int origOutputLimit = output.limit();
      output.limit(output.position() + decompressedSize);
      // Use slices so native API works on independent buffers; advance positions manually.
      Snappy.uncompress(input.slice(), output.slice());
      input.position(input.limit());
      input.limit(origInputLimit);
      output.position(output.limit());
      output.limit(origOutputLimit);
    }

    @Override
    public void release() {}
  }
}
