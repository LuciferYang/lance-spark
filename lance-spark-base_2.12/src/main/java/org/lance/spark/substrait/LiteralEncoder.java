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
package org.lance.spark.substrait;

import com.google.protobuf.ByteString;
import io.substrait.proto.Expression;
import io.substrait.proto.Type;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Translates Spark V2 connector {@link Literal} into Substrait {@link Expression.Literal}.
 *
 * <p>Critical: {@link Literal#value()} is in <em>Spark InternalRow encoding</em>, not the user-
 * friendly form. The encoder must unwrap accordingly:
 *
 * <table>
 *   <caption>Spark literal value JVM types</caption>
 *   <tr><th>Spark {@code DataType}</th><th>{@code lit.value()} JVM type</th></tr>
 *   <tr><td>{@code BooleanType}</td><td>{@code Boolean}</td></tr>
 *   <tr><td>{@code ByteType}/{@code ShortType}/{@code IntegerType}</td>
 *       <td>{@code Byte}/{@code Short}/{@code Integer}</td></tr>
 *   <tr><td>{@code LongType}</td><td>{@code Long}</td></tr>
 *   <tr><td>{@code FloatType}/{@code DoubleType}</td><td>{@code Float}/{@code Double}</td></tr>
 *   <tr><td>{@code StringType}</td>
 *       <td>{@code org.apache.spark.unsafe.types.UTF8String} (NOT {@code String})</td></tr>
 *   <tr><td>{@code BinaryType}</td><td>{@code byte[]}</td></tr>
 *   <tr><td>{@code DateType}</td><td>{@code Integer} (days since 1970-01-01)</td></tr>
 *   <tr><td>{@code TimestampType}/{@code TimestampNTZType}</td>
 *       <td>{@code Long} (microseconds since 1970-01-01)</td></tr>
 *   <tr><td>{@code DecimalType(p,s)}</td>
 *       <td>{@code org.apache.spark.sql.types.Decimal} (NOT {@code java.math.BigDecimal})</td></tr>
 * </table>
 *
 * <p>Decimal encoding follows Substrait spec: 16-byte little-endian two's-complement representation
 * of the unscaled value, with sign-extension padding.
 *
 * <p>This is where the float/double round-trip bug from the legacy SQL-string filter path gets
 * fixed: {@code setFp64(double)} writes the IEEE-754 binary directly. The {@code
 * literalRoundTripsExactBitsForDouble} test pins this against {@code Literal(0.1 + 0.2)}.
 */
public final class LiteralEncoder {

  /** Substrait Decimal value byte width. The spec uses fixed 16 bytes (i128). */
  private static final int DECIMAL_BYTE_WIDTH = 16;

  private LiteralEncoder() {}

  /**
   * Encodes a Spark V2 {@link Literal} into a Substrait {@link Expression.Literal}.
   *
   * @throws IllegalArgumentException if the literal type is not supported (e.g. negative-scale
   *     Decimal, unsupported {@code DataType}, decimal too large for 16 bytes)
   */
  public static Expression.Literal encode(Literal<?> literal) {
    DataType type = literal.dataType();
    Object value = literal.value();
    Expression.Literal.Builder b = Expression.Literal.newBuilder();
    if (value == null) {
      Type typeProto = TypeEncoder.encode(type, true);
      if (typeProto == null) {
        throw new IllegalArgumentException(
            "Cannot encode null literal of unsupported type: " + type);
      }
      return b.setNull(typeProto).setNullable(true).build();
    }
    b.setNullable(false);
    if (type instanceof BooleanType) {
      return b.setBoolean((Boolean) value).build();
    }
    if (type instanceof ByteType) {
      return b.setI8(((Number) value).intValue()).build();
    }
    if (type instanceof ShortType) {
      return b.setI16(((Number) value).intValue()).build();
    }
    if (type instanceof IntegerType) {
      return b.setI32(((Number) value).intValue()).build();
    }
    if (type instanceof LongType) {
      return b.setI64(((Number) value).longValue()).build();
    }
    if (type instanceof FloatType) {
      return b.setFp32(((Number) value).floatValue()).build();
    }
    if (type instanceof DoubleType) {
      return b.setFp64(((Number) value).doubleValue()).build();
    }
    if (type instanceof StringType) {
      // Spark internal repr is UTF8String. Older Spark versions occasionally hand a String
      // directly; tolerate both.
      String s = value instanceof UTF8String ? value.toString() : (String) value;
      return b.setString(s).build();
    }
    if (type instanceof BinaryType) {
      return b.setBinary(ByteString.copyFrom((byte[]) value)).build();
    }
    if (type instanceof DateType) {
      return b.setDate(((Number) value).intValue()).build();
    }
    if (type instanceof TimestampNTZType) {
      return b.setTimestamp(((Number) value).longValue()).build();
    }
    if (type instanceof TimestampType) {
      return b.setTimestampTz(((Number) value).longValue()).build();
    }
    if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      if (dt.scale() < 0) {
        throw new IllegalArgumentException("Substrait Decimal does not accept negative scale");
      }
      BigDecimal bd = ((Decimal) value).toJavaBigDecimal();
      ByteString encoded = ByteString.copyFrom(encodeDecimalLittleEndian(bd, dt.scale()));
      return b.setDecimal(
              Expression.Literal.Decimal.newBuilder()
                  .setPrecision(dt.precision())
                  .setScale(dt.scale())
                  .setValue(encoded))
          .build();
    }
    throw new IllegalArgumentException("Unsupported Spark literal type: " + type);
  }

  /**
   * Encodes the unscaled BigInteger of {@code value * 10^scale} as a 16-byte little-endian
   * two's-complement integer, sign-extended via 0x00 (positive) or 0xFF (negative).
   *
   * <p>Matches Substrait's {@code Expression.Literal.Decimal.value} field convention.
   */
  static byte[] encodeDecimalLittleEndian(BigDecimal value, int scale) {
    BigDecimal scaled = value.setScale(scale, java.math.RoundingMode.UNNECESSARY);
    BigInteger unscaled = scaled.unscaledValue();
    // BigInteger.toByteArray() returns big-endian two's complement, minimum length.
    byte[] beBytes = unscaled.toByteArray();
    if (beBytes.length > DECIMAL_BYTE_WIDTH) {
      throw new IllegalArgumentException(
          "Decimal value " + value + " does not fit in " + DECIMAL_BYTE_WIDTH + " bytes");
    }
    byte sign = unscaled.signum() < 0 ? (byte) 0xFF : 0;
    byte[] out = new byte[DECIMAL_BYTE_WIDTH];
    // Reverse beBytes into the low end of out, then sign-extend the high bytes.
    for (int i = 0; i < beBytes.length; i++) {
      out[i] = beBytes[beBytes.length - 1 - i];
    }
    for (int i = beBytes.length; i < DECIMAL_BYTE_WIDTH; i++) {
      out[i] = sign;
    }
    return out;
  }
}
