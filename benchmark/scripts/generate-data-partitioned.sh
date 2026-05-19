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
# See the License for the specific language governing permissions and
# limitations under the License.

# TPC-DS partitioned Parquet data generation.
#
# Thin wrapper over generate-data.sh that pins FORMATS to partitioned-parquet.
# Fact tables are Hive-partitioned by their date_sk (canonical spark-sql-perf
# convention); dimensions stay flat.
#
# Output layout under ${DATA_DIR}:
#   partitioned-parquet/
#     store_sales/ss_sold_date_sk=2451181/part-*.parquet
#     store_sales/ss_sold_date_sk=2451182/part-*.parquet
#     ...
#     date_dim/part-*.parquet           (unpartitioned dimension)
#
# Usage:
#   ./generate-data-partitioned.sh [SCALE_FACTOR] [SPARK_MASTER]
#
# The FORMATS positional argument is fixed to "partitioned-parquet" and is not
# overridable from this wrapper — use generate-data.sh directly for mixed runs.
#
# Environment variables (passed through to generate-data.sh via exec):
#   DATA_DIR              Output root (default: ../data relative to benchmark/)
#   SPARK_VERSION         Spark profile (default: 3.5)
#   SCALA_VERSION         Scala profile (default: 2.12)
#   SPARK_HOME            Path to spark-submit (default: PATH lookup)
#   DRIVER_MEMORY         spark-submit --driver-memory  (default: 4g)
#   EXECUTOR_MEMORY       spark-submit --executor-memory (default: 4g)
#   MAX_BYTES_PER_FILE    Lance-only; no effect on partitioned-parquet output
#   MAX_ROWS_PER_FILE     Lance-only; no effect on partitioned-parquet output
#   FILE_FORMAT_VERSION   Lance-only; no effect on partitioned-parquet output

set -euo pipefail

if [ "$#" -gt 2 ]; then
  echo "Usage: $0 [SCALE_FACTOR] [SPARK_MASTER] (got $# arguments: $*)" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

SCALE_FACTOR="${1:-1}"
SPARK_MASTER="${2:-local[*]}"

exec "${SCRIPT_DIR}/generate-data.sh" "${SCALE_FACTOR}" partitioned-parquet "${SPARK_MASTER}"
