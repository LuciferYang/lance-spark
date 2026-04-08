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

/**
 * Substrait protobuf encoders for Lance scan pushdown.
 *
 * <p>This package translates Spark V2 connector expressions into the Substrait protobuf format that
 * Lance's scanner consumes via {@code ScanOptions.substraitFilter} and {@code
 * ScanOptions.substraitAggregate}. The encoders are intentionally narrow:
 *
 * <ul>
 *   <li>{@link org.lance.spark.substrait.SubstraitContext} — per-encode mutable state: dataset
 *       schema binding, function/URI anchor registries.
 *   <li>{@link org.lance.spark.substrait.FunctionNames} — static map from Spark V2 op names to bare
 *       Substrait function names (e.g. {@code "="} → {@code "equal"}).
 *   <li>{@link org.lance.spark.substrait.TypeEncoder} — Spark {@code DataType} → Substrait {@code
 *       Type}, including dataset-schema {@code NamedStruct} construction.
 *   <li>{@link org.lance.spark.substrait.LiteralEncoder} — Spark {@code Literal&lt;?&gt;} →
 *       Substrait {@code Expression.Literal}.
 *   <li>{@link org.lance.spark.substrait.PredicateEncoder} — Spark V2 {@code Predicate} → Substrait
 *       {@code ExtendedExpression}. Built on top of the four classes above. (Phase 1.)
 *   <li>{@link org.lance.spark.substrait.AggregateEncoder} — Spark V2 aggregate functions →
 *       Substrait {@code Plan} containing an {@code AggregateRel}. (Phase 2.)
 * </ul>
 *
 * <p>Driver-side only. {@link org.lance.spark.substrait.SubstraitContext} is mutable and per-
 * encode; encoded bytes are serialized into {@code LanceInputPartition} and shipped to workers as
 * {@code byte[]}. Workers never instantiate the encoder.
 *
 * <p>Production deployments use the {@code lance-spark-bundle-*} uber-jars, which relocate {@code
 * io.substrait} → {@code org.lance.spark.shaded.substrait} and {@code com.google.protobuf} → {@code
 * org.lance.spark.shaded.protobuf} so that substrait-java's bundled protobuf does not collide with
 * whatever protobuf version Spark itself ships at runtime.
 */
package org.lance.spark.substrait;
