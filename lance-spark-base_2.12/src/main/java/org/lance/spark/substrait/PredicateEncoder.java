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

/**
 * Translates Spark V2 {@link org.apache.spark.sql.connector.expressions.filter.Predicate} arrays
 * into Substrait {@code ExtendedExpression} bytes for {@code ScanOptions.substraitFilter}.
 *
 * <p><strong>Phase 1 deliverable.</strong> Resolves <a
 * href="https://github.com/lance-format/lance-spark/issues/252">lance-format/lance-spark#252</a>.
 * Not implemented in this Phase 0 PR; this class exists only as a package marker so the design
 * doc's layout is committed.
 */
public final class PredicateEncoder {
  private PredicateEncoder() {}
}
