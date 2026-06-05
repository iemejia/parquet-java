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
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.compression.DefaultCompressionCodecFactory;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;

/**
 * Hadoop-aware wrapper around {@link DefaultCompressionCodecFactory}.
 *
 * <p>This class adds constructors that accept Hadoop {@link Configuration} objects,
 * delegating all compression logic to the parent class which has no Hadoop dependency.
 */
public class CodecFactory extends DefaultCompressionCodecFactory {

  // Keep these constants here for backward compatibility with code that references CodecFactory.PARQUET_COMPRESS_ZSTD_*
  public static final String PARQUET_COMPRESS_ZSTD_LEVEL =
      DefaultCompressionCodecFactory.PARQUET_COMPRESS_ZSTD_LEVEL;
  public static final int DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL =
      DefaultCompressionCodecFactory.DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL;
  public static final String PARQUET_COMPRESS_ZSTD_WORKERS =
      DefaultCompressionCodecFactory.PARQUET_COMPRESS_ZSTD_WORKERS;
  public static final int DEFAULT_PARQUET_COMPRESS_ZSTD_WORKERS =
      DefaultCompressionCodecFactory.DEFAULT_PARQUET_COMPRESS_ZSTD_WORKERS;

  /**
   * Create a new codec factory.
   *
   * @param configuration used to pass compression codec configuration information
   * @param pageSize      the expected page size, does not set a hard limit, currently just
   *                      used to set the initial size of the output stream used when
   *                      compressing a buffer. If this factory is only used to construct
   *                      decompressors this parameter has no impact on the function of the factory
   */
  public CodecFactory(Configuration configuration, int pageSize) {
    super(new HadoopParquetConfiguration(configuration), pageSize);
  }

  /**
   * Create a new codec factory.
   *
   * @param configuration used to pass compression codec configuration information
   * @param pageSize      the expected page size, does not set a hard limit, currently just
   *                      used to set the initial size of the output stream used when
   *                      compressing a buffer. If this factory is only used to construct
   *                      decompressors this parameter has no impact on the function of the factory
   */
  public CodecFactory(ParquetConfiguration configuration, int pageSize) {
    super(configuration, pageSize);
  }

  /**
   * Create a codec factory that will provide compressors and decompressors
   * that will work natively with ByteBuffers backed by direct memory.
   *
   * @param config    configuration options for different compression codecs
   * @param allocator an allocator for creating result buffers during compression
   *                  and decompression, must provide buffers backed by Direct
   *                  memory and return true for the isDirect() method
   *                  on the ByteBufferAllocator interface
   * @param pageSize  the default page size. This does not set a hard limit on the
   *                  size of buffers that can be compressed, but performance may
   *                  be improved by setting it close to the expected size of buffers
   *                  (in the case of parquet, pages) that will be compressed. This
   *                  setting is unused in the case of decompressing data, as parquet
   *                  always records the uncompressed size of a buffer. If this
   *                  CodecFactory is only going to be used for decompressors, this
   *                  parameter will not impact the function of the factory.
   * @return a configured direct codec factory
   */
  public static CodecFactory createDirectCodecFactory(
      Configuration config, ByteBufferAllocator allocator, int pageSize) {
    return new DirectCodecFactory(config, allocator, pageSize);
  }
}
