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
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reflection-based helper for brotli4j (runtime-only dependency).
 * Initialized eagerly at class-load time; all fields are null if
 * brotli4j is not on the classpath.
 *
 * <p>Uses {@code Encoder.compress(byte[], Encoder.Parameters)} for compression and
 * {@code Decoder.decompress(byte[], int, int)} for decompression — the latter returns
 * {@code byte[]} directly and avoids loading {@code DirectDecompress} which references
 * {@code io.netty.buffer.ByteBuf} (optional Netty dependency not on our classpath).
 */
public final class Brotli4j {

  private static final Logger LOG = LoggerFactory.getLogger(Brotli4j.class);

  public static final boolean AVAILABLE;
  // Encoder.compress(byte[], Object/*Encoder.Parameters*/) -> byte[]
  private static final Method COMPRESS;
  // Decoder.decompress(byte[], int/*offset*/, int/*length*/) -> byte[]
  private static final Method DECOMPRESS;
  // Encoder.Parameters class
  private static final Class<?> PARAMS_CLASS;
  // Encoder.Parameters.setQuality(int) -> Encoder.Parameters
  private static final Method SET_QUALITY;

  static {
    boolean loaded = false;
    Method compress = null, decompress = null, setQuality = null;
    Class<?> paramsClass = null;
    try {
      // Load native library
      Class<?> loader = Class.forName("com.aayushatharva.brotli4j.Brotli4jLoader");
      loader.getMethod("ensureAvailability").invoke(null);

      // Encoder.compress(byte[], Encoder.Parameters) -> byte[]
      paramsClass = Class.forName("com.aayushatharva.brotli4j.encoder.Encoder$Parameters");
      Class<?> encoder = Class.forName("com.aayushatharva.brotli4j.encoder.Encoder");
      compress = encoder.getMethod("compress", byte[].class, paramsClass);

      // Decoder.decompress(byte[], int, int) -> byte[]
      // This avoids loading DirectDecompress which references io.netty.buffer.ByteBuf
      Class<?> decoder = Class.forName("com.aayushatharva.brotli4j.decoder.Decoder");
      decompress = decoder.getMethod("decompress", byte[].class, int.class, int.class);

      // Encoder.Parameters.setQuality(int) -> Encoder.Parameters
      setQuality = paramsClass.getMethod("setQuality", int.class);

      loaded = true;
    } catch (Throwable t) {
      // brotli4j not available — BROTLI will not be supported
      LOG.info("brotli4j not available, BROTLI codec will not be supported: {}", t.toString());
    }
    AVAILABLE = loaded;
    COMPRESS = compress;
    DECOMPRESS = decompress;
    PARAMS_CLASS = paramsClass;
    SET_QUALITY = setQuality;
  }

  private Brotli4j() {}

  /** Create an {@code Encoder.Parameters} instance with the given quality. */
  public static Object newParams(int quality) {
    try {
      Object params = PARAMS_CLASS.getConstructor().newInstance();
      SET_QUALITY.invoke(params, quality);
      return params;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to create Brotli encoder parameters", e);
    }
  }

  /** Compress using {@code Encoder.compress(byte[], Encoder.Parameters)}. */
  public static byte[] compress(byte[] input, Object params) throws IOException {
    try {
      return (byte[]) COMPRESS.invoke(null, input, params);
    } catch (ReflectiveOperationException e) {
      throw new IOException("Brotli compression failed", e);
    }
  }

  /** Decompress using {@code Decoder.decompress(byte[], offset, length)}. */
  public static byte[] decompress(byte[] input) throws IOException {
    try {
      return (byte[]) DECOMPRESS.invoke(null, input, 0, input.length);
    } catch (ReflectiveOperationException e) {
      throw new IOException("Brotli decompression failed", e);
    }
  }
}
