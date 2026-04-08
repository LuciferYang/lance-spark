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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Static map from Spark V2 op names (as returned by {@link
 * org.apache.spark.sql.connector.expressions.GeneralScalarExpression#name()} and {@link
 * org.apache.spark.sql.connector.expressions.filter.Predicate#name()}) to bare Substrait function
 * names plus the standard extension URI the function lives in.
 *
 * <p>Function names are intentionally bare (e.g. {@code "equal"}) rather than signature-mangled
 * (e.g. {@code "equal:any_any"}) — Lance's {@code datafusion-substrait} consumer accepts the bare
 * form, verified against the test fixtures in {@code lance-mine/rust/lance-datafusion/src/
 * substrait.rs:881}.
 *
 * <p>The boolean operators {@code AND}, {@code OR}, {@code NOT} are <em>not</em> in this map; they
 * are special-cased structurally by {@link PredicateEncoder} (which still calls {@link
 * SubstraitContext#registerFunction} so the {@code functions_boolean.yaml} URI ends up in the
 * envelope). The same applies to {@code IN} (synthesized as OR-of-equals), {@code CAST} (top- level
 * expression class), and the constant predicates {@code ALWAYS_TRUE}, {@code ALWAYS_FALSE}, {@code
 * BOOLEAN_EXPRESSION}.
 */
public final class FunctionNames {

  /** Bare Substrait function name plus the standard extension URI it is declared in. */
  public static final class FunctionRef {
    private final String bareName;
    private final String extensionUri;

    public FunctionRef(String bareName, String extensionUri) {
      this.bareName = bareName;
      this.extensionUri = extensionUri;
    }

    public String bareName() {
      return bareName;
    }

    public String extensionUri() {
      return extensionUri;
    }
  }

  /** Standard extension URI for {@code functions_comparison.yaml}. */
  public static final String COMPARISON_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml";

  /** Standard extension URI for {@code functions_arithmetic.yaml}. */
  public static final String ARITHMETIC_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml";

  /** Standard extension URI for {@code functions_boolean.yaml}. */
  public static final String BOOLEAN_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/functions_boolean.yaml";

  /** Standard extension URI for {@code functions_string.yaml}. */
  public static final String STRING_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/functions_string.yaml";

  /** Standard extension URI for {@code functions_aggregate_generic.yaml}. */
  public static final String AGGREGATE_GENERIC_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/functions_aggregate_generic.yaml";

  private static final Map<String, FunctionRef> SCALAR_OPS = new HashMap<>();
  private static final Map<String, FunctionRef> AGGREGATE_OPS = new HashMap<>();

  static {
    // Comparison (functions_comparison.yaml)
    SCALAR_OPS.put("=", new FunctionRef("equal", COMPARISON_URI));
    SCALAR_OPS.put("<>", new FunctionRef("not_equal", COMPARISON_URI));
    SCALAR_OPS.put("<=>", new FunctionRef("is_not_distinct_from", COMPARISON_URI));
    SCALAR_OPS.put(">", new FunctionRef("gt", COMPARISON_URI));
    SCALAR_OPS.put(">=", new FunctionRef("gte", COMPARISON_URI));
    SCALAR_OPS.put("<", new FunctionRef("lt", COMPARISON_URI));
    SCALAR_OPS.put("<=", new FunctionRef("lte", COMPARISON_URI));
    SCALAR_OPS.put("IS_NULL", new FunctionRef("is_null", COMPARISON_URI));
    SCALAR_OPS.put("IS_NOT_NULL", new FunctionRef("is_not_null", COMPARISON_URI));

    // Arithmetic (functions_arithmetic.yaml)
    SCALAR_OPS.put("+", new FunctionRef("add", ARITHMETIC_URI));
    SCALAR_OPS.put("-", new FunctionRef("subtract", ARITHMETIC_URI));
    SCALAR_OPS.put("*", new FunctionRef("multiply", ARITHMETIC_URI));
    SCALAR_OPS.put("/", new FunctionRef("divide", ARITHMETIC_URI));

    // String (functions_string.yaml)
    SCALAR_OPS.put("STARTS_WITH", new FunctionRef("starts_with", STRING_URI));
    SCALAR_OPS.put("ENDS_WITH", new FunctionRef("ends_with", STRING_URI));
    SCALAR_OPS.put("CONTAINS", new FunctionRef("contains", STRING_URI));
    SCALAR_OPS.put("UPPER", new FunctionRef("upper", STRING_URI));
    SCALAR_OPS.put("LOWER", new FunctionRef("lower", STRING_URI));

    // Phase-2 aggregates. The substrait spec splits these between two YAML extensions:
    //   * count lives in functions_aggregate_generic.yaml (along with any_value)
    //   * sum, min, max, avg live in functions_arithmetic.yaml
    // AVG is intentionally absent from this map — see AggregateEncoder for the algebraic-monoid
    // argument.
    AGGREGATE_OPS.put("sum", new FunctionRef("sum", ARITHMETIC_URI));
    AGGREGATE_OPS.put("count", new FunctionRef("count", AGGREGATE_GENERIC_URI));
    AGGREGATE_OPS.put("min", new FunctionRef("min", ARITHMETIC_URI));
    AGGREGATE_OPS.put("max", new FunctionRef("max", ARITHMETIC_URI));
  }

  private FunctionNames() {}

  /**
   * Returns the Substrait function reference for a Spark V2 scalar op name (e.g. {@code "="},
   * {@code ">"}, {@code "+"}, {@code "STARTS_WITH"}).
   *
   * @return the {@link FunctionRef}, or {@link Optional#empty()} if the op is not supported by this
   *     encoder.
   */
  public static Optional<FunctionRef> lookupScalar(String sparkOpName) {
    return Optional.ofNullable(SCALAR_OPS.get(sparkOpName));
  }

  /**
   * Returns the Substrait function reference for a Spark V2 aggregate function name (e.g. {@code
   * "sum"}, {@code "count"}, {@code "min"}).
   *
   * @return the {@link FunctionRef}, or {@link Optional#empty()} if the function is not supported
   *     by this encoder. Notably, {@code "avg"} is not supported in Phase 2 — see {@link
   *     AggregateEncoder} for the algebraic-monoid argument.
   */
  public static Optional<FunctionRef> lookupAggregate(String sparkAggName) {
    return Optional.ofNullable(AGGREGATE_OPS.get(sparkAggName));
  }
}
