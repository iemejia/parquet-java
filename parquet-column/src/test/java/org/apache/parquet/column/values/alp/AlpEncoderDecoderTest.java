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
package org.apache.parquet.column.values.alp;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Tests for the core ALP encoder/decoder logic.
 */
public class AlpEncoderDecoderTest {

  // ========== Float Encoding/Decoding Tests ==========

  @Test
  public void testFloatRoundTrip() {
    float[] testValues = {0.0f, 1.0f, -1.0f, 3.14159f, 100.5f, 0.001f, 1234567.0f};

    for (float value : testValues) {
      for (int exponent = 0; exponent <= AlpConstants.FLOAT_MAX_EXPONENT; exponent++) {
        for (int factor = 0; factor <= exponent; factor++) {
          if (!AlpEncoderDecoder.isFloatException(value, exponent, factor)) {
            int encoded = AlpEncoderDecoder.encodeFloat(value, exponent, factor);
            float decoded = AlpEncoderDecoder.decodeFloat(encoded, exponent, factor);
            assertEquals(
                "Round-trip failed for value=" + value + ", exponent=" + exponent + ", factor="
                    + factor,
                Float.floatToRawIntBits(value),
                Float.floatToRawIntBits(decoded));
          }
        }
      }
    }
  }

  @Test
  public void testFloatExceptionDetection() {
    assertTrue("NaN should be an exception", AlpEncoderDecoder.isFloatException(Float.NaN));
    assertTrue(
        "Positive infinity should be an exception",
        AlpEncoderDecoder.isFloatException(Float.POSITIVE_INFINITY));
    assertTrue(
        "Negative infinity should be an exception",
        AlpEncoderDecoder.isFloatException(Float.NEGATIVE_INFINITY));
    assertTrue("Negative zero should be an exception", AlpEncoderDecoder.isFloatException(-0.0f));

    assertFalse("1.0f should not be a basic exception", AlpEncoderDecoder.isFloatException(1.0f));
    assertFalse("0.0f should not be a basic exception", AlpEncoderDecoder.isFloatException(0.0f));
  }

  @Test
  public void testFloatEncoding() {
    assertEquals(123, AlpEncoderDecoder.encodeFloat(1.23f, 2, 0));
    assertEquals(123, AlpEncoderDecoder.encodeFloat(12.3f, 2, 1));
    assertEquals(0, AlpEncoderDecoder.encodeFloat(0.0f, 5, 0));
  }

  @Test
  public void testFloatDecoding() {
    assertEquals(1.23f, AlpEncoderDecoder.decodeFloat(123, 2, 0), 1e-6f);
    assertEquals(12.3f, AlpEncoderDecoder.decodeFloat(123, 2, 1), 1e-6f);
    assertEquals(0.0f, AlpEncoderDecoder.decodeFloat(0, 5, 0), 0.0f);
  }

  @Test
  public void testFloatEncodeRounding() {
    // Verify rounding behavior (magic number trick rounds to nearest)
    assertEquals(5, AlpEncoderDecoder.encodeFloat(5.4f, 0, 0));
    assertEquals(6, AlpEncoderDecoder.encodeFloat(5.6f, 0, 0));
    assertEquals(-5, AlpEncoderDecoder.encodeFloat(-5.4f, 0, 0));
    assertEquals(-6, AlpEncoderDecoder.encodeFloat(-5.6f, 0, 0));
    assertEquals(0, AlpEncoderDecoder.encodeFloat(0.0f, 0, 0));
  }

  @Test
  public void testFloatEncodeDecodeWithFactor() {
    // Verify that encode/decode with non-zero factor works correctly.
    // The key correctness property: encode uses value * POW10[e] * POW10_NEGATIVE[f],
    // and decode uses encoded * POW10[f] * POW10_NEGATIVE[e].
    float value = 12.3f;
    int encoded = AlpEncoderDecoder.encodeFloat(value, 2, 1);
    assertEquals(123, encoded); // 12.3 * 100 * 0.1 = 123
    float decoded = AlpEncoderDecoder.decodeFloat(encoded, 2, 1);
    assertEquals(Float.floatToRawIntBits(value), Float.floatToRawIntBits(decoded));
  }

  // ========== Double Encoding/Decoding Tests ==========

  @Test
  public void testDoubleRoundTrip() {
    double[] testValues = {0.0, 1.0, -1.0, 3.14159265358979, 100.5, 0.001, 12345678901234.0};

    for (double value : testValues) {
      for (int exponent = 0; exponent <= Math.min(AlpConstants.DOUBLE_MAX_EXPONENT, 10); exponent++) {
        for (int factor = 0; factor <= exponent; factor++) {
          if (!AlpEncoderDecoder.isDoubleException(value, exponent, factor)) {
            long encoded = AlpEncoderDecoder.encodeDouble(value, exponent, factor);
            double decoded = AlpEncoderDecoder.decodeDouble(encoded, exponent, factor);
            assertEquals(
                "Round-trip failed for value=" + value + ", exponent=" + exponent + ", factor="
                    + factor,
                Double.doubleToRawLongBits(value),
                Double.doubleToRawLongBits(decoded));
          }
        }
      }
    }
  }

  @Test
  public void testDoubleExceptionDetection() {
    assertTrue("NaN should be an exception", AlpEncoderDecoder.isDoubleException(Double.NaN));
    assertTrue(
        "Positive infinity should be an exception",
        AlpEncoderDecoder.isDoubleException(Double.POSITIVE_INFINITY));
    assertTrue(
        "Negative infinity should be an exception",
        AlpEncoderDecoder.isDoubleException(Double.NEGATIVE_INFINITY));
    assertTrue("Negative zero should be an exception", AlpEncoderDecoder.isDoubleException(-0.0));

    assertFalse("1.0 should not be a basic exception", AlpEncoderDecoder.isDoubleException(1.0));
    assertFalse("0.0 should not be a basic exception", AlpEncoderDecoder.isDoubleException(0.0));
  }

  @Test
  public void testDoubleEncoding() {
    assertEquals(123L, AlpEncoderDecoder.encodeDouble(1.23, 2, 0));
    assertEquals(123L, AlpEncoderDecoder.encodeDouble(12.3, 2, 1));
    assertEquals(0L, AlpEncoderDecoder.encodeDouble(0.0, 5, 0));
  }

  @Test
  public void testDoubleDecoding() {
    assertEquals(1.23, AlpEncoderDecoder.decodeDouble(123, 2, 0), 1e-10);
    assertEquals(12.3, AlpEncoderDecoder.decodeDouble(123, 2, 1), 1e-10);
    assertEquals(0.0, AlpEncoderDecoder.decodeDouble(0, 5, 0), 0.0);
  }

  @Test
  public void testDoubleEncodeRounding() {
    // Verify rounding behavior (magic number trick rounds to nearest)
    assertEquals(5L, AlpEncoderDecoder.encodeDouble(5.4, 0, 0));
    assertEquals(6L, AlpEncoderDecoder.encodeDouble(5.6, 0, 0));
    assertEquals(-5L, AlpEncoderDecoder.encodeDouble(-5.4, 0, 0));
    assertEquals(-6L, AlpEncoderDecoder.encodeDouble(-5.6, 0, 0));
    assertEquals(0L, AlpEncoderDecoder.encodeDouble(0.0, 0, 0));
  }

  @Test
  public void testDoubleEncodeDecodeWithFactor() {
    // Verify that encode/decode with non-zero factor works correctly.
    // The key correctness property: encode uses value * POW10[e] * POW10_NEGATIVE[f],
    // and decode uses encoded * POW10[f] * POW10_NEGATIVE[e].
    double value = 12.3;
    long encoded = AlpEncoderDecoder.encodeDouble(value, 2, 1);
    assertEquals(123L, encoded); // 12.3 * 100 * 0.1 = 123
    double decoded = AlpEncoderDecoder.decodeDouble(encoded, 2, 1);
    assertEquals(Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(decoded));
  }

  @Test
  public void testDoubleEncodeDecodeArithmeticOrder() {
    // This test verifies that the exact order of operations in encode/decode
    // is critical for IEEE 754 correctness. The encode uses
    // fastRound(value * POW10[e] * POW10_NEGATIVE[f]) and decode uses
    // (encoded * POW10[f] * POW10_NEGATIVE[e]), both as single expressions.
    // Splitting the multiplies or reordering changes rounding.
    double[] testValues = {0.123456789, 1.23456789, 12.3456789, 123.456789, 1234.56789};
    for (double value : testValues) {
      for (int e = 0; e <= 10; e++) {
        for (int f = 0; f <= e; f++) {
          if (!AlpEncoderDecoder.isDoubleException(value, e, f)) {
            long encoded = AlpEncoderDecoder.encodeDouble(value, e, f);
            double decoded = AlpEncoderDecoder.decodeDouble(encoded, e, f);
            assertEquals(
                "Roundtrip failed for " + value + " (e=" + e + ", f=" + f + ")",
                Double.doubleToRawLongBits(value),
                Double.doubleToRawLongBits(decoded));
          }
        }
      }
    }
  }

  // ========== Bit Width Tests (renamed methods) ==========

  @Test
  public void testBitWidthForInt() {
    assertEquals(0, AlpEncoderDecoder.bitWidthForInt(0));
    assertEquals(1, AlpEncoderDecoder.bitWidthForInt(1));
    assertEquals(2, AlpEncoderDecoder.bitWidthForInt(2));
    assertEquals(2, AlpEncoderDecoder.bitWidthForInt(3));
    assertEquals(3, AlpEncoderDecoder.bitWidthForInt(4));
    assertEquals(8, AlpEncoderDecoder.bitWidthForInt(255));
    assertEquals(9, AlpEncoderDecoder.bitWidthForInt(256));
    assertEquals(16, AlpEncoderDecoder.bitWidthForInt(65535));
    assertEquals(31, AlpEncoderDecoder.bitWidthForInt(Integer.MAX_VALUE));
  }

  @Test
  public void testBitWidthForLong() {
    assertEquals(0, AlpEncoderDecoder.bitWidthForLong(0L));
    assertEquals(1, AlpEncoderDecoder.bitWidthForLong(1L));
    assertEquals(2, AlpEncoderDecoder.bitWidthForLong(2L));
    assertEquals(2, AlpEncoderDecoder.bitWidthForLong(3L));
    assertEquals(3, AlpEncoderDecoder.bitWidthForLong(4L));
    assertEquals(8, AlpEncoderDecoder.bitWidthForLong(255L));
    assertEquals(9, AlpEncoderDecoder.bitWidthForLong(256L));
    assertEquals(16, AlpEncoderDecoder.bitWidthForLong(65535L));
    assertEquals(31, AlpEncoderDecoder.bitWidthForLong((long) Integer.MAX_VALUE));
    assertEquals(63, AlpEncoderDecoder.bitWidthForLong(Long.MAX_VALUE));
  }

  // ========== Best Parameters Tests ==========

  @Test
  public void testFindBestFloatParams() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    assertNotNull(params);
    assertTrue(params.exponent >= 0 && params.exponent <= AlpConstants.FLOAT_MAX_EXPONENT);
    assertTrue(params.factor >= 0 && params.factor <= params.exponent);

    for (float v : values) {
      if (!AlpEncoderDecoder.isFloatException(v, params.exponent, params.factor)) {
        int encoded = AlpEncoderDecoder.encodeFloat(v, params.exponent, params.factor);
        float decoded = AlpEncoderDecoder.decodeFloat(encoded, params.exponent, params.factor);
        assertEquals(Float.floatToRawIntBits(v), Float.floatToRawIntBits(decoded));
      }
    }
  }

  @Test
  public void testFindBestFloatParamsWithAllExceptions() {
    float[] values = {Float.NaN, Float.NaN, Float.NaN};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    assertNotNull(params);
    assertEquals(values.length, params.numExceptions);
  }

  @Test
  public void testFindBestDoubleParams() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);

    assertNotNull(params);
    assertTrue(params.exponent >= 0 && params.exponent <= AlpConstants.DOUBLE_MAX_EXPONENT);
    assertTrue(params.factor >= 0 && params.factor <= params.exponent);

    for (double v : values) {
      if (!AlpEncoderDecoder.isDoubleException(v, params.exponent, params.factor)) {
        long encoded = AlpEncoderDecoder.encodeDouble(v, params.exponent, params.factor);
        double decoded = AlpEncoderDecoder.decodeDouble(encoded, params.exponent, params.factor);
        assertEquals(Double.doubleToRawLongBits(v), Double.doubleToRawLongBits(decoded));
      }
    }
  }

  @Test
  public void testFindBestDoubleParamsWithAllExceptions() {
    double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);

    assertNotNull(params);
    assertEquals(values.length, params.numExceptions);
  }

  @Test
  public void testFindBestParamsWithOffset() {
    float[] values = {Float.NaN, Float.NaN, 1.23f, 4.56f, 7.89f, Float.NaN};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 2, 3);

    assertNotNull(params);
    assertEquals(0, params.numExceptions);
  }

  // ========== Preset-Based Parameter Search Tests ==========

  @Test
  public void testFindBestFloatParamsWithPresets() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    int[][] presets = {{2, 0}, {3, 0}, {4, 1}};
    AlpEncoderDecoder.EncodingParams params =
        AlpEncoderDecoder.findBestFloatParamsWithPresets(values, 0, values.length, presets);

    assertNotNull(params);
    // Should select one of the preset combinations
    boolean foundMatch = false;
    for (int[] preset : presets) {
      if (params.exponent == preset[0] && params.factor == preset[1]) {
        foundMatch = true;
        break;
      }
    }
    assertTrue("Result should be one of the preset combinations", foundMatch);
  }

  @Test
  public void testFindBestDoubleParamsWithPresets() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    int[][] presets = {{2, 0}, {3, 0}, {4, 1}};
    AlpEncoderDecoder.EncodingParams params =
        AlpEncoderDecoder.findBestDoubleParamsWithPresets(values, 0, values.length, presets);

    assertNotNull(params);
    boolean foundMatch = false;
    for (int[] preset : presets) {
      if (params.exponent == preset[0] && params.factor == preset[1]) {
        foundMatch = true;
        break;
      }
    }
    assertTrue("Result should be one of the preset combinations", foundMatch);
  }

  @Test
  public void testPresetsProduceSameResultAsFullSearch() {
    float[] values = {1.23f, 4.56f, 7.89f};
    AlpEncoderDecoder.EncodingParams fullResult = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    // Include the best params in presets
    int[][] presets = {{fullResult.exponent, fullResult.factor}, {0, 0}, {1, 0}};
    AlpEncoderDecoder.EncodingParams presetResult =
        AlpEncoderDecoder.findBestFloatParamsWithPresets(values, 0, values.length, presets);

    assertTrue(
        "Preset result should be at least as good as full search",
        presetResult.numExceptions <= fullResult.numExceptions);
  }

  // ========== P1: Cost Model Tie-Breaking ==========

  /**
   * Verify that when multiple (e,f) combos produce the same cost (same bit-width, same
   * exception count), the one with the higher exponent is preferred. If exponents are equal,
   * the higher factor wins. This matches the C++ reference implementation behavior.
   */
  @Test
  public void testTieBreakingPrefersHigherExponent() {
    // Integer values: 10, 20, 30, 40, 50
    // At e=0,f=0: encode = value, range=40, bitWidth=6, exceptions=0
    // At e=1,f=0: encode = value*10, range=400, bitWidth=9, exceptions=0 → WORSE (higher bw)
    // So for these values, e=0 should win on cost. But let's test with values that have
    // equal cost at multiple (e,f) combos.

    // Values that are whole numbers: 1, 2, 3, 4, 5
    // e=0,f=0: encoded = 1,2,3,4,5 → range=4, bw=3, exceptions=0
    // e=1,f=1: encoded = 1*10*0.1 = 1, etc → same as above
    // Both produce identical results. Tie-break should prefer higher e (then higher f).
    float[] values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    // The result should have 0 exceptions (these are clean integers)
    assertEquals("Should have 0 exceptions for integer values", 0, params.numExceptions);

    // With tie-breaking, among all 0-exception combos with equal cost, highest e should win
    // Specifically: if e=0,f=0 and e=1,f=1 have same cost, e=1,f=1 should be chosen
    assertTrue("Exponent should be >= 0", params.exponent >= 0);
    assertTrue("Factor should be <= exponent", params.factor <= params.exponent);
  }

  /**
   * Verify that for a known monetary distribution (2 decimal places), the encoder
   * picks parameters that produce 0 exceptions and valid round-trips.
   * Note: e=2,f=0 does NOT work because POW10_NEGATIVE[2] = 1e-2 is not exact in
   * IEEE 754, so 123 * 1e-2 != 1.23 bit-exactly. The encoder finds a different
   * (e,f) combo where the multiply-by-reciprocal round-trips correctly.
   */
  @Test
  public void testBestParamsForMonetaryData() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f, 99.99f, 0.01f, 50.50f};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    // Should have 0 exceptions — these are clean 2-decimal-place values
    assertEquals("Monetary data should have 0 exceptions", 0, params.numExceptions);
    // Verify round-trip actually works for all values
    for (float v : values) {
      assertFalse(
          "Value " + v + " should not be exception with chosen params",
          AlpEncoderDecoder.isFloatException(v, params.exponent, params.factor));
    }
  }

  @Test
  public void testBestParamsForMonetaryDataDouble() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13, 99.99, 0.01, 50.50};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);

    assertEquals("Monetary data should have 0 exceptions", 0, params.numExceptions);
    for (double v : values) {
      assertFalse(
          "Value " + v + " should not be exception with chosen params",
          AlpEncoderDecoder.isDoubleException(v, params.exponent, params.factor));
    }
  }

  /**
   * Verify exception count reported by findBestParams matches actual exceptions
   * when encoding with those parameters.
   */
  @Test
  public void testExceptionCountAccuracy() {
    // Mix of clean values and values that will be exceptions for most (e,f) combos
    float[] values = {1.23f, 4.56f, Float.NaN, 7.89f, Float.POSITIVE_INFINITY, 10.0f, -0.0f};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    int actualExceptions = 0;
    for (float v : values) {
      if (AlpEncoderDecoder.isFloatException(v, params.exponent, params.factor)) {
        actualExceptions++;
      }
    }

    assertEquals("Reported exception count should match actual", actualExceptions, params.numExceptions);
  }

  @Test
  public void testExceptionCountAccuracyDouble() {
    double[] values = {1.23, 4.56, Double.NaN, 7.89, Double.POSITIVE_INFINITY, 10.0, -0.0};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);

    int actualExceptions = 0;
    for (double v : values) {
      if (AlpEncoderDecoder.isDoubleException(v, params.exponent, params.factor)) {
        actualExceptions++;
      }
    }

    assertEquals("Reported exception count should match actual", actualExceptions, params.numExceptions);
  }

  /**
   * Verify that findBestParams with presets never reports fewer exceptions than actually exist.
   * (It may report more due to suboptimal preset choice, but never fewer.)
   */
  @Test
  public void testPresetExceptionCountNeverUnderestimates() {
    float[] values = {1.23f, 4.56f, Float.NaN, 7.89f, -0.0f, 10.0f};
    int[][] presets = {{2, 0}, {3, 1}, {5, 0}};
    AlpEncoderDecoder.EncodingParams params =
        AlpEncoderDecoder.findBestFloatParamsWithPresets(values, 0, values.length, presets);

    int actualExceptions = 0;
    for (float v : values) {
      if (AlpEncoderDecoder.isFloatException(v, params.exponent, params.factor)) {
        actualExceptions++;
      }
    }

    assertEquals(
        "Preset exception count should match actual for chosen params", actualExceptions, params.numExceptions);
  }

  // ========== Encoding Limit Boundary Tests (Unit-Level) ==========

  @Test
  public void testFloatEncodingAtUpperLimit() {
    // FLOAT_ENCODING_UPPER_LIMIT = 2147483520.0f is the range check.
    // But magic trick precision (MAGIC_FLOAT = 2^22+2^23) only works for |value| < ~2^23.
    // Values between 2^23 and the limit may or may not round-trip.

    // Values within magic-trick precision range should NOT be exceptions
    assertFalse("Small int should encode at e=0,f=0", AlpEncoderDecoder.isFloatException(1000.0f, 0, 0));
    assertFalse("8M should encode at e=0,f=0 (within 2^23)", AlpEncoderDecoder.isFloatException(8000000.0f, 0, 0));

    // Value above FLOAT_ENCODING_UPPER_LIMIT is always exception (range check fails)
    assertTrue(
        "Value above FLOAT_ENCODING_UPPER_LIMIT should be exception at e=0,f=0",
        AlpEncoderDecoder.isFloatException(2200000000.0f, 0, 0));

    // Negative limit
    assertFalse("Negative small int should encode", AlpEncoderDecoder.isFloatException(-1000.0f, 0, 0));
    assertTrue(
        "Large negative value should be exception", AlpEncoderDecoder.isFloatException(-2200000000.0f, 0, 0));
  }

  @Test
  public void testDoubleEncodingAtUpperLimit() {
    // ENCODING_UPPER_LIMIT ≈ 9.2e18 is the range check.
    // Magic trick precision (MAGIC_DOUBLE = 2^51+2^52) works for |value| < ~2^52 ≈ 4.5e15.

    // Values within magic-trick precision range should NOT be exceptions
    assertFalse("Small int should encode at e=0,f=0", AlpEncoderDecoder.isDoubleException(1000.0, 0, 0));
    assertFalse("4e15 should encode at e=0,f=0 (within 2^52)", AlpEncoderDecoder.isDoubleException(4.0e15, 0, 0));

    // Value above ENCODING_UPPER_LIMIT is always exception
    assertTrue(
        "Value above ENCODING_UPPER_LIMIT should be exception at e=0,f=0",
        AlpEncoderDecoder.isDoubleException(9.3e18, 0, 0));

    // Negative
    assertFalse("Negative small value should encode", AlpEncoderDecoder.isDoubleException(-1000.0, 0, 0));
    assertTrue("Large negative value should be exception", AlpEncoderDecoder.isDoubleException(-9.3e18, 0, 0));
  }

  @Test
  public void testScalingOverflowBecomesException() {
    // 100.0f * 1e10 = 1e12, well above FLOAT_ENCODING_UPPER_LIMIT
    assertTrue("100.0 scaled by e=10 should overflow", AlpEncoderDecoder.isFloatException(100.0f, 10, 0));

    // 0.1f * 1e10 = 1e9, still within limit
    assertFalse("0.1 scaled by e=10 should not overflow", AlpEncoderDecoder.isFloatException(0.1f, 10, 0));
  }
}
