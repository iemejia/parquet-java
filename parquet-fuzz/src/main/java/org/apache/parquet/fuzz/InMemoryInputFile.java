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
package org.apache.parquet.fuzz;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * An in-memory implementation of {@link InputFile} backed by a byte array.
 * Used by fuzz tests to feed arbitrary bytes to {@code ParquetFileReader}.
 */
public class InMemoryInputFile implements InputFile {

  private final byte[] data;

  public InMemoryInputFile(byte[] data) {
    this.data = data;
  }

  @Override
  public long getLength() {
    return data.length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new InMemorySeekableInputStream(data);
  }

  private static final class InMemorySeekableInputStream extends SeekableInputStream {

    private final byte[] data;
    private int pos = 0;

    InMemorySeekableInputStream(byte[] data) {
      this.data = data;
    }

    @Override
    public int read() throws IOException {
      if (pos >= data.length) {
        return -1;
      }
      return data[pos++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (pos >= data.length) {
        return -1;
      }
      int available = Math.min(len, data.length - pos);
      System.arraycopy(data, pos, b, off, available);
      pos += available;
      return available;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0 || newPos > data.length) {
        throw new IOException("Seek position " + newPos + " is out of range [0, " + data.length + "]");
      }
      this.pos = (int) newPos;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      if (data.length - pos < len) {
        throw new EOFException(
            "Reached end of stream with " + (data.length - pos) + " bytes available; needed " + len);
      }
      System.arraycopy(data, pos, bytes, start, len);
      pos += len;
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      if (pos >= data.length) {
        return -1;
      }
      int available = Math.min(buf.remaining(), data.length - pos);
      buf.put(data, pos, available);
      pos += available;
      return available;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      int len = buf.remaining();
      if (data.length - pos < len) {
        throw new EOFException(
            "Reached end of stream with " + (data.length - pos) + " bytes available; needed " + len);
      }
      buf.put(data, pos, len);
      pos += len;
    }

    @Override
    public void close() {
      // nothing to close for in-memory data
    }
  }
}
