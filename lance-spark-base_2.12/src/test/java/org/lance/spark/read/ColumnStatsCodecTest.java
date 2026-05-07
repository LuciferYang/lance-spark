/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.read;

import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Round-trip tests for {@link ColumnStatsCodec}. */
class ColumnStatsCodecTest {

  @Test
  @DisplayName("Long round-trip")
  void longRoundTrip() {
    String encoded = ColumnStatsCodec.encode(123456789L, DataTypes.LongType);
    assertTrue(encoded.startsWith("i64:"));
    assertEquals(123456789L, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Integer round-trip")
  void integerRoundTrip() {
    String encoded = ColumnStatsCodec.encode(42, DataTypes.IntegerType);
    assertTrue(encoded.startsWith("i32:"));
    assertEquals(42, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Short round-trip")
  void shortRoundTrip() {
    short v = 1234;
    String encoded = ColumnStatsCodec.encode(v, DataTypes.ShortType);
    assertEquals(v, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Byte round-trip")
  void byteRoundTrip() {
    byte v = 7;
    String encoded = ColumnStatsCodec.encode(v, DataTypes.ByteType);
    assertEquals(v, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Float round-trip")
  void floatRoundTrip() {
    String encoded = ColumnStatsCodec.encode(3.14f, DataTypes.FloatType);
    assertEquals(3.14f, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Double round-trip")
  void doubleRoundTrip() {
    String encoded = ColumnStatsCodec.encode(2.71828d, DataTypes.DoubleType);
    assertEquals(2.71828d, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Boolean round-trip")
  void booleanRoundTrip() {
    assertEquals(
        true, ColumnStatsCodec.decode(ColumnStatsCodec.encode(true, DataTypes.BooleanType)));
    assertEquals(
        false, ColumnStatsCodec.decode(ColumnStatsCodec.encode(false, DataTypes.BooleanType)));
  }

  @Test
  @DisplayName("String round-trip")
  void stringRoundTrip() {
    String original = "hello world";
    String encoded = ColumnStatsCodec.encode(original, DataTypes.StringType);
    assertTrue(encoded.startsWith("utf8:"));
    assertEquals(original, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("String with delimiter survives encoding")
  void stringWithDelimiter() {
    String original = "value:with:colons:and ' quotes";
    String encoded = ColumnStatsCodec.encode(original, DataTypes.StringType);
    assertEquals(original, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("String with non-ASCII")
  void stringNonAscii() {
    String original = "你好,世界 🌍";
    String encoded = ColumnStatsCodec.encode(original, DataTypes.StringType);
    assertEquals(original, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Binary round-trip")
  void binaryRoundTrip() {
    byte[] original = new byte[] {0x00, 0x7f, (byte) 0xff, 0x42};
    String encoded = ColumnStatsCodec.encode(original, DataTypes.BinaryType);
    assertTrue(encoded.startsWith("bin:"));
    assertArrayEquals(original, (byte[]) ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Decimal round-trip")
  void decimalRoundTrip() {
    BigDecimal original = new BigDecimal("12345.6789");
    String encoded = ColumnStatsCodec.encode(original, DataTypes.createDecimalType(10, 4));
    assertTrue(encoded.startsWith("dec:"));
    assertEquals(original, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("null value encodes to null")
  void nullValue() {
    assertNull(ColumnStatsCodec.encode(null, DataTypes.LongType));
  }

  @Test
  @DisplayName("malformed encoded string decodes to null instead of throwing")
  void malformedDecodes() {
    assertNull(ColumnStatsCodec.decode(null));
    assertNull(ColumnStatsCodec.decode("nodelimiter"));
    assertNull(ColumnStatsCodec.decode("unknownTag:42"));
  }

  @Test
  @DisplayName("Float NaN encodes to null (NaN bound would mis-order in CBO)")
  void floatNanReturnsNullOnEncode() {
    // R11: NaN as a bound makes CBO compute empty ranges (NaN compares > everything in
    // Float.compare). Codec returns null, signalling the writer should skip the stat.
    assertNull(ColumnStatsCodec.encode(Float.NaN, DataTypes.FloatType));
  }

  @Test
  @DisplayName("Double NaN encodes to null")
  void doubleNanReturnsNullOnEncode() {
    assertNull(ColumnStatsCodec.encode(Double.NaN, DataTypes.DoubleType));
  }

  @Test
  @DisplayName("Float / Double Infinity encodes to null")
  void infinityReturnsNullOnEncode() {
    assertNull(ColumnStatsCodec.encode(Float.POSITIVE_INFINITY, DataTypes.FloatType));
    assertNull(ColumnStatsCodec.encode(Float.NEGATIVE_INFINITY, DataTypes.FloatType));
    assertNull(ColumnStatsCodec.encode(Double.POSITIVE_INFINITY, DataTypes.DoubleType));
    assertNull(ColumnStatsCodec.encode(Double.NEGATIVE_INFINITY, DataTypes.DoubleType));
  }

  @Test
  @DisplayName("Decode of poisoned f32:NaN / f64:Infinity returns null (symmetric guard)")
  void decodeNonFiniteFloatReturnsNull() {
    assertNull(ColumnStatsCodec.decode("f32:NaN"));
    assertNull(ColumnStatsCodec.decode("f32:Infinity"));
    assertNull(ColumnStatsCodec.decode("f32:-Infinity"));
    assertNull(ColumnStatsCodec.decode("f64:NaN"));
    assertNull(ColumnStatsCodec.decode("f64:Infinity"));
  }

  @Test
  @DisplayName("Empty string round-trip")
  void emptyStringRoundTrip() {
    String encoded = ColumnStatsCodec.encode("", DataTypes.StringType);
    assertEquals("", ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Zero-length binary round-trip")
  void zeroLengthBinaryRoundTrip() {
    byte[] empty = new byte[0];
    String encoded = ColumnStatsCodec.encode(empty, DataTypes.BinaryType);
    Object decoded = ColumnStatsCodec.decode(encoded);
    assertNotNull(decoded);
    assertEquals(0, ((byte[]) decoded).length);
  }

  @Test
  @DisplayName("Decimal with negative scale round-trip")
  void decimalNegativeScaleRoundTrip() {
    BigDecimal original = new BigDecimal(new java.math.BigInteger("100"), -2); // = 10000
    String encoded = ColumnStatsCodec.encode(original, DataTypes.createDecimalType(10, 0));
    BigDecimal decoded = (BigDecimal) ColumnStatsCodec.decode(encoded);
    assertEquals(0, original.compareTo(decoded));
    assertEquals(-2, decoded.scale());
  }

  @Test
  @DisplayName("Date round-trip returns Integer days-since-epoch")
  void dateRoundTrip() {
    java.sql.Date original = java.sql.Date.valueOf("2026-04-30");
    String encoded = ColumnStatsCodec.encode(original, DataTypes.DateType);
    Object decoded = ColumnStatsCodec.decode(encoded);
    assertTrue(decoded instanceof Integer);
    assertEquals((int) original.toLocalDate().toEpochDay(), decoded);
  }

  @Test
  @DisplayName("Oversized base64 utf8 body returns null instead of allocating")
  void oversizedUtf8BodyReturnsNull() {
    StringBuilder sb = new StringBuilder("utf8:");
    for (int i = 0; i < 70_000; i++) {
      sb.append('A');
    }
    assertNull(ColumnStatsCodec.decode(sb.toString()));
  }

  @Test
  @DisplayName("Oversized base64 binary body returns null instead of allocating")
  void oversizedBinaryBodyReturnsNull() {
    StringBuilder sb = new StringBuilder("bin:");
    for (int i = 0; i < 70_000; i++) {
      sb.append('A');
    }
    assertNull(ColumnStatsCodec.decode(sb.toString()));
  }

  @Test
  @DisplayName("Out-of-range date encoded value returns null on decode")
  void dateOutOfRangeReturnsNull() {
    long farFuture = (long) Integer.MAX_VALUE + 100L;
    assertNull(ColumnStatsCodec.decode("date:" + farFuture));
    long farPast = (long) Integer.MIN_VALUE - 100L;
    assertNull(ColumnStatsCodec.decode("date:" + farPast));
  }

  @Test
  @DisplayName("Long.MIN_VALUE / Long.MAX_VALUE round-trip")
  void longMinMaxRoundTrip() {
    assertEquals(
        Long.MAX_VALUE,
        ColumnStatsCodec.decode(ColumnStatsCodec.encode(Long.MAX_VALUE, DataTypes.LongType)));
    assertEquals(
        Long.MIN_VALUE,
        ColumnStatsCodec.decode(ColumnStatsCodec.encode(Long.MIN_VALUE, DataTypes.LongType)));
  }

  @Test
  @DisplayName("Oversized decimal unscaled body returns null instead of allocating BigInteger")
  void oversizedDecimalBodyReturnsNull() {
    // A poisoned property with a multi-thousand-digit unscaled value would otherwise trigger
    // O(n^2) BigInteger string parsing on every scan. Spark's DecimalType maxes at precision 38,
    // so any body > 64 chars is illegitimate.
    StringBuilder sb = new StringBuilder("dec:0:");
    for (int i = 0; i < 100_000; i++) {
      sb.append('1');
    }
    assertNull(ColumnStatsCodec.decode(sb.toString()));
  }

  @Test
  @DisplayName("Oversized decimal scale prefix returns null")
  void oversizedDecimalScaleReturnsNull() {
    // A massive scale prefix is also a poisoning vector — bound it independently from the body.
    StringBuilder sb = new StringBuilder("dec:");
    for (int i = 0; i < 50; i++) {
      sb.append('9');
    }
    sb.append(":1");
    assertNull(ColumnStatsCodec.decode(sb.toString()));
  }

  @Test
  @DisplayName("Decimal with max-precision (38 digits) round-trips within bound")
  void decimalMaxPrecisionRoundTrip() {
    // Within the documented bound (Spark's DecimalType.MAX_PRECISION = 38). Must still decode.
    BigDecimal max = new BigDecimal(new java.math.BigInteger("9".repeat(38)), 0);
    String encoded = ColumnStatsCodec.encode(max, DataTypes.createDecimalType(38, 0));
    BigDecimal decoded = (BigDecimal) ColumnStatsCodec.decode(encoded);
    assertEquals(0, max.compareTo(decoded));
  }

  @Test
  @DisplayName("Malformed numeric body returns null per contract (no NumberFormatException)")
  void malformedNumericBodyReturnsNull() {
    // The numeric arms used to throw NumberFormatException, violating the documented null-
    // return contract. Caller (LanceScanBuilder.loadPersistedColumnStats) catches Exception
    // generically, but a tighter contract here makes the codec safe for any caller and lets
    // the test verify the exception did not leak.
    assertNull(ColumnStatsCodec.decode("i32:not_a_number"));
    assertNull(ColumnStatsCodec.decode("i64:abc"));
    assertNull(ColumnStatsCodec.decode("i16:99999999999"));
    assertNull(ColumnStatsCodec.decode("i8:300"));
    assertNull(ColumnStatsCodec.decode("f32:NaNNN"));
    assertNull(ColumnStatsCodec.decode("f64:1.2.3"));
    assertNull(ColumnStatsCodec.decode("ts:notalong"));
    assertNull(ColumnStatsCodec.decode("tsntz:!@#"));
    assertNull(ColumnStatsCodec.decode("date:abc"));
  }

  @Test
  @DisplayName("Oversized tag returns null without allocating the giant prefix string")
  void oversizedTagReturnsNull() {
    // A 100 KB tag would otherwise materialize on the heap and flow into the unrecognized-tag
    // debug log. The MAX_TAG_LEN cap fires on `sep` (the index of ':') before the substring
    // is allocated.
    StringBuilder sb = new StringBuilder(100_000);
    for (int i = 0; i < 100_000; i++) {
      sb.append('A');
    }
    sb.append(":body");
    assertNull(ColumnStatsCodec.decode(sb.toString()));
  }

  @Test
  @DisplayName("Decode returns null (not exception) for malformed numeric / date / decimal bodies")
  void decodeMalformedReturnsNull() {
    // Each input below has a recognized tag but a malformed body. The codec contract states
    // null on malformed input rather than propagating NumberFormatException to the caller.
    assertNull(ColumnStatsCodec.decode("i32:"));
    assertNull(ColumnStatsCodec.decode("f64:"));
    assertNull(ColumnStatsCodec.decode(":"));
    assertNull(ColumnStatsCodec.decode("::"));
    assertNull(ColumnStatsCodec.decode("i32:0x10"));
    assertNull(ColumnStatsCodec.decode("tsntz:"));
    assertNull(ColumnStatsCodec.decode("dec::1"));
    assertNull(ColumnStatsCodec.decode("dec:1:notanumber"));
    assertNull(ColumnStatsCodec.decode("dec:abc:1"));
  }

  @Test
  @DisplayName("Timestamp round-trip preserves microsecond precision")
  void timestampRoundTrip() {
    // 2026-04-30 12:34:56.789123 UTC: 1493: Spark stores timestamps as micros-since-epoch.
    java.sql.Timestamp original = java.sql.Timestamp.valueOf("2026-04-30 12:34:56.789123");
    String encoded = ColumnStatsCodec.encode(original, DataTypes.TimestampType);
    assertTrue(encoded.startsWith("ts:"));
    Object decoded = ColumnStatsCodec.decode(encoded);
    assertTrue(decoded instanceof Long, "Expected Long micros, got " + decoded.getClass());
    long expectedMicros = original.getTime() * 1000L + (original.getNanos() % 1_000_000) / 1000L;
    assertEquals(expectedMicros, decoded);
  }

  @Test
  @DisplayName("Timestamp round-trip with sub-microsecond nanos truncates correctly")
  void timestampSubMicrosecondNanos() {
    // 0 millis + 123_456_789 nanos = 123_456 micros (789 sub-µs trailing nanos discarded).
    java.sql.Timestamp original = new java.sql.Timestamp(0L);
    original.setNanos(123_456_789);
    String encoded = ColumnStatsCodec.encode(original, DataTypes.TimestampType);
    Object decoded = ColumnStatsCodec.decode(encoded);
    // Spark truncates (not rounds) to micros, so 789 ns trailing → 123_456 µs.
    assertEquals(123_456L, decoded);
  }

  @Test
  @DisplayName("TimestampNTZ round-trip via java.time.LocalDateTime")
  void timestampNtzRoundTrip() {
    java.time.LocalDateTime ldt = java.time.LocalDateTime.of(2026, 4, 30, 12, 34, 56, 789_000_000);
    String encoded = ColumnStatsCodec.encode(ldt, DataTypes.TimestampNTZType);
    assertTrue(encoded.startsWith("tsntz:"));
    Object decoded = ColumnStatsCodec.decode(encoded);
    assertTrue(decoded instanceof Long);
    long expectedMicros =
        ldt.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L + ldt.getNano() / 1000L;
    assertEquals(expectedMicros, decoded);
  }

  @Test
  @DisplayName("BinaryType encode rejects non-byte[] value (defensive cast guard)")
  void binaryEncodeRejectsNonByteArray() {
    // Spark's Row#get for BinaryType returns byte[], but the codec defensively returns null
    // on a type mismatch rather than throwing ClassCastException — see R10 review.
    assertNull(ColumnStatsCodec.encode("not-a-byte-array", DataTypes.BinaryType));
    assertNull(ColumnStatsCodec.encode(42, DataTypes.BinaryType));
  }

  @Test
  @DisplayName("FloatType encode rejects non-Float value (R17 instanceof guard)")
  void floatEncodeRejectsNonFloat() {
    // Defensive: a Double passed for FloatType returns null, NOT a ClassCastException.
    assertNull(ColumnStatsCodec.encode(1.5d, DataTypes.FloatType));
    assertNull(ColumnStatsCodec.encode(42, DataTypes.FloatType));
    assertNull(ColumnStatsCodec.encode("1.5", DataTypes.FloatType));
  }

  @Test
  @DisplayName("DoubleType encode rejects non-Double value (R17 instanceof guard)")
  void doubleEncodeRejectsNonDouble() {
    // A Float passed for DoubleType, or any other Number subtype, returns null.
    assertNull(ColumnStatsCodec.encode(1.5f, DataTypes.DoubleType));
    assertNull(ColumnStatsCodec.encode(42L, DataTypes.DoubleType));
    assertNull(ColumnStatsCodec.encode("2.5", DataTypes.DoubleType));
  }

  @Test
  @DisplayName("Bool decode: 'true'/'false' (case-insensitive) round-trip; all other bodies → null")
  void decodeBoolRequiresTrueOrFalse() {
    // R11 tightening: Boolean.parseBoolean's permissive semantics let "bool:yes" → false,
    // which an attacker could plant to bias CBO selectivity. Codec now requires exact strings.
    assertEquals(true, ColumnStatsCodec.decode("bool:true"));
    assertEquals(false, ColumnStatsCodec.decode("bool:false"));
    assertEquals(true, ColumnStatsCodec.decode("bool:TRUE"));
    assertEquals(false, ColumnStatsCodec.decode("bool:False"));
    // All non-{true,false} bodies return null (was: silently false under parseBoolean).
    assertNull(ColumnStatsCodec.decode("bool:"));
    assertNull(ColumnStatsCodec.decode("bool:yes"));
    assertNull(ColumnStatsCodec.decode("bool:1"));
    assertNull(ColumnStatsCodec.decode("bool:notabool"));
  }

  @Test
  @DisplayName("DateType encode handles java.time.LocalDate (Java8 datetime API)")
  void dateEncodesLocalDate() {
    // When spark.sql.datetime.java8API.enabled=true, Row#get for DateType returns LocalDate.
    java.time.LocalDate ld = java.time.LocalDate.of(2026, 4, 30);
    String encoded = ColumnStatsCodec.encode(ld, DataTypes.DateType);
    assertTrue(encoded.startsWith("date:"));
    assertEquals((int) ld.toEpochDay(), ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("TimestampType encode handles java.time.Instant (Java8 datetime API)")
  void timestampEncodesInstant() {
    java.time.Instant ins = java.time.Instant.parse("2026-04-30T12:34:56.789012Z");
    String encoded = ColumnStatsCodec.encode(ins, DataTypes.TimestampType);
    assertTrue(encoded.startsWith("ts:"));
    long expectedMicros = ins.getEpochSecond() * 1_000_000L + ins.getNano() / 1000L;
    assertEquals(expectedMicros, ColumnStatsCodec.decode(encoded));
  }

  @Test
  @DisplayName("Date / Timestamp / TimestampNTZ encode returns null on unrecognized runtime type")
  void datetimeEncodeNullOnUnknownType() {
    // Pre-R11 fallback was `value.toString()`, producing strings the decoder cannot parse.
    // Now: any other type returns null per the documented null-on-unsupported contract.
    assertNull(ColumnStatsCodec.encode("2026-04-30", DataTypes.DateType));
    assertNull(ColumnStatsCodec.encode("2026-04-30T12:00:00Z", DataTypes.TimestampType));
    assertNull(ColumnStatsCodec.encode("not-a-datetime", DataTypes.TimestampNTZType));
  }

  @Test
  @DisplayName("Oversized numeric body returns null (DoS guard against poisoned long bodies)")
  void oversizedNumericBodyReturnsNull() {
    StringBuilder sb = new StringBuilder("f32:");
    for (int i = 0; i < 100; i++) {
      sb.append('9');
    }
    // Body is 100 chars > MAX_NUMERIC_BODY_LEN=64; decode returns null without invoking parseFloat.
    assertNull(ColumnStatsCodec.decode(sb.toString()));
    // Same guard for ints.
    StringBuilder bigInt = new StringBuilder("i64:");
    for (int i = 0; i < 100; i++) {
      bigInt.append('1');
    }
    assertNull(ColumnStatsCodec.decode(bigInt.toString()));
  }

  @Test
  @DisplayName(
      "Invalid base64 body returns null (codec contract over Base64.IllegalArgumentException)")
  void invalidBase64ReturnsNull() {
    // Base64.getDecoder().decode throws IllegalArgumentException on alphabet violations; the
    // codec catches it and returns null per its documented contract.
    assertNull(ColumnStatsCodec.decode("utf8:!!not_base64!!"));
    assertNull(ColumnStatsCodec.decode("bin: spaces and stuff "));
  }
}
