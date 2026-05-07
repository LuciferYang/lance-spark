# Lance Spark Micro Benchmark

JMH-based micro-benchmarks comparing **Lance** vs **Parquet** data source read performance for OLAP-oriented workloads in Apache Spark.

## Benchmark Scenarios

| # | Scenario | Data Size | Key Metric |
|---|----------|-----------|------------|
| 1 | Numeric Column Scan | 15M rows | Raw columnar scan throughput (int/long/double) |
| 2 | Wide Table Column Pruning | 1M rows × 10/50/100 cols | Column pruning efficiency |
| 3 | Int + String Scan | 10M rows | Mixed-type scan |
| 4 | String with Nulls | 10M rows | Null handling (0%/50%/95% nulls) |
| 5 | Predicate Filter | 10M rows | Filter pushdown (low ~0.1% / high ~50% selectivity) |
| 6 | Multi-Column Aggregation | 20M rows | GROUP BY + SUM/AVG |
| 7 | TopN Query | 20M rows | ORDER BY ... LIMIT (Lance TopN push-down) |
| 8 | Range Filter | 10M rows | BETWEEN on ordered column (~5% range) |

## Prerequisites

- JDK 11+
- Maven 3.6+
- Lance-spark bundle installed to local Maven repository

## Build

```bash
# 1. First install lance-spark bundle to local Maven repo (from project root)
cd /path/to/lance-spark
mvn install -pl lance-spark-bundle-3.5_2.12 -am -DskipTests

# 2. Build the micro-benchmark module
cd micro-benchmark
mvn clean package
```

## Run

### Via Maven (recommended)

```bash
# Run all benchmarks
cd micro-benchmark
mvn exec:exec

# Run specific benchmark
mvn exec:exec -Djmh.benchmarks=".*numericScan.*"

# Run with specific parameters
mvn exec:exec -Djmh.benchmarks=".*wideTable.*"
```

### Quick validation (fast mode)

For custom JMH args (warmup/iteration/fork overrides), run the JMH Main class directly:

```bash
# Build classpath and run with custom JMH args
mvn package -q
java -Xms4g -Xmx4g -XX:+UseG1GC \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -Djdk.reflect.useDirectMethodHandle=false \
  -Dio.netty.tryReflectionSetAccessible=true \
  -cp "target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)" \
  org.openjdk.jmh.Main ".*numericScan.*" -wi 0 -i 1 -f 1 -p format=parquet -p dataType=int
```

## JMH Parameters

| Parameter | Values | Applies To |
|-----------|--------|------------|
| `format` | `lance`, `parquet` | All scenarios |
| `dataType` | `int`, `long`, `double` | numericScan |
| `numColumns` | `10`, `50`, `100` | wideTableColumnPruning |
| `nullFraction` | `0.0`, `0.5`, `0.95` | stringWithNullsScan |
| `selectivity` | `low`, `high` | predicateFilter |

## Configuration

Default JMH settings (defined in annotations):
- **Warmup:** 5 iterations × 10s
- **Measurement:** 5 iterations × 10s
- **Forks:** 1 (use `-f 3` for publishable results)
- **JVM Memory:** 4GB
- **GC:** G1GC

## Output

JMH produces a table like:

```
Benchmark                                          (format)  (dataType)  Mode  Cnt    Score   Error  Units
DataSourceReadBenchmark.numericScan                   lance         int  avgt    5  123.456 ± 5.678  ms/op
DataSourceReadBenchmark.numericScan                 parquet         int  avgt    5  145.789 ± 6.012  ms/op
```

Export to JSON for further analysis (requires direct java invocation):
```bash
java ... org.openjdk.jmh.Main ".*DataSourceReadBenchmark.*" -rf json -rff results.json
```

## Design Notes

- Data is generated once per `(format, params)` combination in `@Setup(Level.Trial)`
- Queries execute via Spark's `noop` sink to force full plan execution without I/O overhead
- SparkSession runs in `local[*]` with AQE disabled for deterministic plans
- Parquet uses Snappy compression with vectorized reader enabled (Spark defaults)
- Lance uses its built-in default compression (zstd) — deliberate production-default comparison
- SparkSession singleton is cleared between trials to prevent cross-contamination
- A warmup query runs during `@Setup` to stabilize Spark codegen before measurement
- Filter benchmark data is shuffled (`ORDER BY rand()`) to avoid sorted-data bias in statistics
- For publishable results, use `-f 3` (3 forks minimum) via direct java invocation
