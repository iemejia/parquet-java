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
 *
 * @deprecated Use {@link DefaultCompressionCodecFactory} directly with {@link ParquetConfiguration}.
 */
@Deprecated
public class CodecFactory extends DefaultCompressionCodecFactory {

  public static final String PARQUET_COMPRESS_ZSTD_LEVEL =
      DefaultCompressionCodecFactory.PARQUET_COMPRESS_ZSTD_LEVEL;
  public static final int DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL =
      DefaultCompressionCodecFactory.DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL;
  public static final String PARQUET_COMPRESS_ZSTD_WORKERS =
      DefaultCompressionCodecFactory.PARQUET_COMPRESS_ZSTD_WORKERS;
  public static final int DEFAULT_PARQUET_COMPRESS_ZSTD_WORKERS =
      DefaultCompressionCodecFactory.DEFAULT_PARQUET_COMPRESS_ZSTD_WORKERS;

  public CodecFactory(Configuration configuration, int pageSize) {
    super(new HadoopParquetConfiguration(configuration), pageSize);
  }

  public CodecFactory(ParquetConfiguration configuration, int pageSize) {
    super(configuration, pageSize);
  }

  /**
   * @deprecated Use {@link DefaultCompressionCodecFactory#createDirectCodecFactory(ParquetConfiguration, ByteBufferAllocator, int)} instead.
   */
  @Deprecated
  public static DefaultCompressionCodecFactory createDirectCodecFactory(
      Configuration config, ByteBufferAllocator allocator, int pageSize) {
    return DefaultCompressionCodecFactory.createDirectCodecFactory(
        new HadoopParquetConfiguration(config), allocator, pageSize);
  }
}
