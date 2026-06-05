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

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * LZ4_RAW compression codec using airlift LZ4 directly with reusable direct ByteBuffers.
 */
final class Lz4RawCodec {

  private Lz4RawCodec() {}

  /**
   * Compresses using airlift's LZ4 compressor directly with reusable direct ByteBuffers.
   */
  static final class Compressor extends DefaultCompressionCodecFactory.BytesCompressor {
    private final Lz4Compressor compressor = new Lz4Compressor();
    private ByteBuffer directInputBuf;
    private ByteBuffer directOutputBuf;

    Compressor() {}

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      int maxLen = compressor.maxCompressedLength(input.length);

      // Grow reusable direct input buffer if needed
      if (directInputBuf == null || directInputBuf.capacity() < input.length) {
        directInputBuf = ByteBuffer.allocateDirect(input.length);
      }
      directInputBuf.clear();
      directInputBuf.put(input);
      directInputBuf.flip();

      // Grow reusable direct output buffer if needed
      if (directOutputBuf == null || directOutputBuf.capacity() < maxLen) {
        directOutputBuf = ByteBuffer.allocateDirect(maxLen);
      }
      directOutputBuf.clear();

      compressor.compress(directInputBuf, directOutputBuf);
      int compressedSize = directOutputBuf.position();

      // Copy result to heap byte array
      directOutputBuf.flip();
      byte[] output = new byte[compressedSize];
      directOutputBuf.get(output);
      return BytesInput.from(output, 0, compressedSize);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.LZ4_RAW;
    }

    @Override
    public void release() {
      directInputBuf = null;
      directOutputBuf = null;
    }
  }

  /**
   * Decompresses using airlift's LZ4 decompressor with reusable direct ByteBuffers.
   * Aircompressor's LZ4 native implementation is significantly faster (~25%) on direct
   * (off-heap) buffers because it can use raw native pointers via Unsafe, avoiding the
   * overhead of JNI array pinning required for heap-backed buffers. The cost of copying
   * data to/from the reusable direct buffers is more than offset by the faster native
   * decompression, especially at typical Parquet page sizes (64KB-1MB).
   */
  static final class Decompressor extends DefaultCompressionCodecFactory.BytesDecompressor {
    private final Lz4Decompressor decompressor = new Lz4Decompressor();
    private ByteBuffer directInputBuf;
    private ByteBuffer directOutputBuf;

    Decompressor() {}

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      int inputSize = Math.toIntExact(bytes.size());

      // Grow reusable direct input buffer if needed
      if (directInputBuf == null || directInputBuf.capacity() < inputSize) {
        directInputBuf = ByteBuffer.allocateDirect(inputSize);
      }
      directInputBuf.clear().limit(inputSize);
      // toByteArray() is zero-copy for ByteArrayBytesInput (returns backing array directly)
      directInputBuf.put(bytes.toByteArray(), 0, inputSize);
      directInputBuf.flip();

      // Grow reusable direct output buffer if needed
      if (directOutputBuf == null || directOutputBuf.capacity() < decompressedSize) {
        directOutputBuf = ByteBuffer.allocateDirect(decompressedSize);
      }
      directOutputBuf.clear().limit(decompressedSize);

      decompressor.decompress(directInputBuf.slice(), directOutputBuf.slice());

      // Copy result to heap — returning a ByteArrayBytesInput allows callers to
      // get the byte[] via toByteArray() without an additional copy.
      byte[] output = new byte[decompressedSize];
      directOutputBuf.position(0).limit(decompressedSize);
      directOutputBuf.get(output);
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
      decompressor.decompress(input.slice(), output.slice());
      input.position(input.limit());
      input.limit(origInputLimit);
      output.position(output.limit());
      output.limit(origOutputLimit);
    }

    @Override
    public void release() {
      directInputBuf = null;
      directOutputBuf = null;
    }
  }
}
