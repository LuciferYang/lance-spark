#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# Run TpcdsCboMetricsRunner against the Lance dataset, capturing per-query
# EXPLAIN COST + physical plan + runtime + shuffle metrics for the CBO
# enablement plan.
#
# Required env:
#   SPARK_HOME      — path to the Spark client (defaults to /Users/yangjie01/Tools/spark-4.1.1-bin-hadoop3)
#   JAVA_HOME       — Java 17 install (defaults to /Users/yangjie01/Tools/zulu17)
#
# Optional env:
#   SPARK_VERSION   — defaults to 4.1
#   SCALA_VERSION   — defaults to 2.13
#   DATA_DIR        — defaults to <benchmark>/data
#   RESULTS_DIR     — defaults to <benchmark>/results
#   QUERIES         — comma-separated subset (e.g. q3,q19,q47); empty = all
#   ITERATIONS      — runs per query (default 1)
#   RUN_LABEL       — label for the run dir (default: derived timestamp)
#   SPARK_MASTER    — defaults to local[*]
#   DRIVER_MEMORY   — defaults to 6g
#   EXECUTOR_MEMORY — defaults to 6g

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."
REPO_ROOT="${BENCHMARK_DIR}/.."

SPARK_VERSION="${SPARK_VERSION:-4.1}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
SPARK_HOME="${SPARK_HOME:-/Users/yangjie01/Tools/spark-4.1.1-bin-hadoop3}"
JAVA_HOME_LOCAL="${JAVA_HOME:-/Users/yangjie01/Tools/zulu17}"

export JAVA_HOME="${JAVA_HOME_LOCAL}"
export PATH="${JAVA_HOME}/bin:${PATH}"

DATA_DIR="${DATA_DIR:-${BENCHMARK_DIR}/data}"
RESULTS_DIR="${RESULTS_DIR:-${BENCHMARK_DIR}/results}"
QUERIES="${QUERIES:-}"
ITERATIONS="${ITERATIONS:-1}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"
RUN_LABEL="${RUN_LABEL:-}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DRIVER_MEMORY="${DRIVER_MEMORY:-6g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-6g}"

echo "=== TPC-DS CBO Metrics ==="
echo "SPARK_HOME:       ${SPARK_HOME}"
echo "JAVA_HOME:        ${JAVA_HOME}"
echo "Spark version:    ${SPARK_VERSION}"
echo "Scala version:    ${SCALA_VERSION}"
echo "Data dir:         ${DATA_DIR}"
echo "Results dir:      ${RESULTS_DIR}"
echo "Run label:        ${RUN_LABEL:-(auto)}"
echo "Iterations:       ${ITERATIONS}"
echo "Queries:          ${QUERIES:-(all)}"
echo ""

# Build benchmark jar against the requested Spark/Scala combo
BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
echo "--- Building benchmark jar (SPARK_VERSION=${SPARK_VERSION} SCALA_VERSION=${SCALA_VERSION}) ---"
cd "${BENCHMARK_DIR}"
"${REPO_ROOT}/mvnw" package -DskipTests -q \
  -Dspark.compat.version="${SPARK_VERSION}" \
  -Dscala.compat.version="${SCALA_VERSION}"
cd "${SCRIPT_DIR}"

# Find the bundle jar
BUNDLE_JAR=$(find "${REPO_ROOT}" -path "*/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
if [ -z "${BUNDLE_JAR}" ]; then
  echo "ERROR: lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION} jar not found."
  echo "Build it first: make bundle SPARK_VERSION=${SPARK_VERSION} SCALA_VERSION=${SCALA_VERSION}"
  exit 1
fi
echo "Bundle jar:       ${BUNDLE_JAR}"
echo "Benchmark jar:    ${BENCHMARK_JAR}"
echo ""

mkdir -p "${RESULTS_DIR}"

EXTRA_ARGS=""
if [ -n "${QUERIES}" ]; then
  EXTRA_ARGS="${EXTRA_ARGS} --queries ${QUERIES}"
fi
if [ -n "${RUN_LABEL}" ]; then
  EXTRA_ARGS="${EXTRA_ARGS} --run-label ${RUN_LABEL}"
fi

# Optional extra --conf flags injected by callers (e.g., kill-switches).
# Format: space-separated "key1=val1 key2=val2".
EXTRA_CONF_ARGS=""
if [ -n "${EXTRA_CONF:-}" ]; then
  for kv in ${EXTRA_CONF}; do
    EXTRA_CONF_ARGS="${EXTRA_CONF_ARGS} --conf ${kv}"
  done
fi

"${SPARK_HOME}/bin/spark-submit" \
  --class org.lance.spark.benchmark.TpcdsCboMetricsRunner \
  --master "${SPARK_MASTER}" \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  --jars "${BUNDLE_JAR}" \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  --conf spark.executor.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  ${EXTRA_CONF_ARGS} \
  "${BENCHMARK_JAR}" \
  --data-dir "${DATA_DIR}" \
  --results-dir "${RESULTS_DIR}" \
  --iterations "${ITERATIONS}" \
  --timeout-seconds "${TIMEOUT_SECONDS}"${EXTRA_ARGS}

echo ""
echo "=== Run complete ==="
echo "Outputs under: ${RESULTS_DIR}/cbo-runs/"
