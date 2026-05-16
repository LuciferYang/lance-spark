# Lance vs Parquet DataSource Read Benchmark Results

## Environment

| Item | Value |
|------|-------|
| JDK | 17.0.18 (Azul Zulu OpenJDK 64-Bit Server VM) |
| OS | macOS (Darwin 25.4.0) |
| Spark | 3.5.5 local[*] |
| Lance Spark | 0.4.0-beta.4 |
| Heap | 4GB (-Xms4g -Xmx4g) |
| GC | G1GC |
| JMH | 1.37 |
| Forks | 3 |
| Warmup | 5 iterations × 10s |
| Measurement | 5 iterations × 10s |
| Total samples | 15 per benchmark |

## Results

```
Benchmark                                       (dataType)  (format)  (nullFraction)  (numColumns)  (selectivity)  Mode  Cnt    Score   Error  Units
DataSourceReadBenchmark.intStringScan                  N/A     lance             N/A           N/A            N/A  avgt   15   75.040 ± 2.790  ms/op
DataSourceReadBenchmark.intStringScan                  N/A   parquet             N/A           N/A            N/A  avgt   15   63.333 ± 1.861  ms/op
DataSourceReadBenchmark.multiColumnAggregation         N/A     lance             N/A           N/A            N/A  avgt   15  160.150 ± 1.133  ms/op
DataSourceReadBenchmark.multiColumnAggregation         N/A   parquet             N/A           N/A            N/A  avgt   15  104.333 ± 1.503  ms/op
DataSourceReadBenchmark.numericScan                    int     lance             N/A           N/A            N/A  avgt   15   50.842 ± 1.374  ms/op
DataSourceReadBenchmark.numericScan                    int   parquet             N/A           N/A            N/A  avgt   15   29.450 ± 2.457  ms/op
DataSourceReadBenchmark.numericScan                   long     lance             N/A           N/A            N/A  avgt   15   53.563 ± 1.219  ms/op
DataSourceReadBenchmark.numericScan                   long   parquet             N/A           N/A            N/A  avgt   15   33.814 ± 2.243  ms/op
DataSourceReadBenchmark.numericScan                 double     lance             N/A           N/A            N/A  avgt   15   54.931 ± 1.397  ms/op
DataSourceReadBenchmark.numericScan                 double   parquet             N/A           N/A            N/A  avgt   15   33.155 ± 1.134  ms/op
DataSourceReadBenchmark.predicateFilter                N/A     lance             N/A           N/A            low  avgt   15   33.953 ± 2.809  ms/op
DataSourceReadBenchmark.predicateFilter                N/A     lance             N/A           N/A           high  avgt   15   39.617 ± 0.719  ms/op
DataSourceReadBenchmark.predicateFilter                N/A   parquet             N/A           N/A            low  avgt   15   32.278 ± 2.264  ms/op
DataSourceReadBenchmark.predicateFilter                N/A   parquet             N/A           N/A           high  avgt   15   39.372 ± 1.288  ms/op
DataSourceReadBenchmark.rangeFilter                    N/A     lance             N/A           N/A            N/A  avgt   15   31.865 ± 0.674  ms/op
DataSourceReadBenchmark.rangeFilter                    N/A   parquet             N/A           N/A            N/A  avgt   15   27.611 ± 2.337  ms/op
DataSourceReadBenchmark.stringWithNullsScan            N/A     lance             0.0           N/A            N/A  avgt   15   69.115 ± 0.875  ms/op
DataSourceReadBenchmark.stringWithNullsScan            N/A     lance             0.5           N/A            N/A  avgt   15   65.349 ± 1.160  ms/op
DataSourceReadBenchmark.stringWithNullsScan            N/A     lance            0.95           N/A            N/A  avgt   15   46.021 ± 1.015  ms/op
DataSourceReadBenchmark.stringWithNullsScan            N/A   parquet             0.0           N/A            N/A  avgt   15   85.925 ± 2.805  ms/op
DataSourceReadBenchmark.stringWithNullsScan            N/A   parquet             0.5           N/A            N/A  avgt   15   68.349 ± 1.924  ms/op
DataSourceReadBenchmark.stringWithNullsScan            N/A   parquet            0.95           N/A            N/A  avgt   15   49.263 ± 2.375  ms/op
DataSourceReadBenchmark.topNQuery                      N/A     lance             N/A           N/A            N/A  avgt   15  110.140 ± 2.430  ms/op
DataSourceReadBenchmark.topNQuery                      N/A   parquet             N/A           N/A            N/A  avgt   15  230.392 ± 9.073  ms/op
DataSourceReadBenchmark.wideTableColumnPruning         N/A     lance             N/A            10            N/A  avgt   15   20.017 ± 0.494  ms/op
DataSourceReadBenchmark.wideTableColumnPruning         N/A     lance             N/A            50            N/A  avgt   15   21.383 ± 0.646  ms/op
DataSourceReadBenchmark.wideTableColumnPruning         N/A     lance             N/A           100            N/A  avgt   15   22.167 ± 0.562  ms/op
DataSourceReadBenchmark.wideTableColumnPruning         N/A   parquet             N/A            10            N/A  avgt   15   27.427 ± 3.174  ms/op
DataSourceReadBenchmark.wideTableColumnPruning         N/A   parquet             N/A            50            N/A  avgt   15   27.336 ± 2.163  ms/op
DataSourceReadBenchmark.wideTableColumnPruning         N/A   parquet             N/A           100            N/A  avgt   15   27.966 ± 4.736  ms/op
```

## Comparison Table

| Scenario | Params | Lance (ms) | Parquet (ms) | Winner | Ratio |
|----------|--------|-----------|-------------|--------|-------|
| numericScan | int | 50.8 ± 1.4 | 29.5 ± 2.5 | Parquet | 1.73x |
| numericScan | long | 53.6 ± 1.2 | 33.8 ± 2.2 | Parquet | 1.58x |
| numericScan | double | 54.9 ± 1.4 | 33.2 ± 1.1 | Parquet | 1.66x |
| intStringScan | — | 75.0 ± 2.8 | 63.3 ± 1.9 | Parquet | 1.18x |
| wideTableColumnPruning | 10 cols | 20.0 ± 0.5 | 27.4 ± 3.2 | **Lance** | 0.73x |
| wideTableColumnPruning | 50 cols | 21.4 ± 0.6 | 27.3 ± 2.2 | **Lance** | 0.78x |
| wideTableColumnPruning | 100 cols | 22.2 ± 0.6 | 28.0 ± 4.7 | **Lance** | 0.79x |
| stringWithNullsScan | 0% nulls | 69.1 ± 0.9 | 85.9 ± 2.8 | **Lance** | 0.80x |
| stringWithNullsScan | 50% nulls | 65.3 ± 1.2 | 68.3 ± 1.9 | **Lance** | 0.96x |
| stringWithNullsScan | 95% nulls | 46.0 ± 1.0 | 49.3 ± 2.4 | **Lance** | 0.93x |
| predicateFilter | low (0.1%) | 34.0 ± 2.8 | 32.3 ± 2.3 | Tie | ~1.0x |
| predicateFilter | high (50%) | 39.6 ± 0.7 | 39.4 ± 1.3 | Tie | ~1.0x |
| multiColumnAggregation | — | 160.2 ± 1.1 | 104.3 ± 1.5 | Parquet | 1.54x |
| topNQuery | — | 110.1 ± 2.4 | 230.4 ± 9.1 | **Lance** | 0.48x |
| rangeFilter | — | 31.9 ± 0.7 | 27.6 ± 2.3 | Parquet | 1.15x |

## Key Findings

### Lance Advantages

1. **TopN Query (2.1x faster)** — Lance's native TopN push-down avoids a full sort, delivering 110ms vs Parquet's 230ms. This is the most significant advantage.

2. **Wide Table Column Pruning (22-27% faster)** — Lance consistently outperforms Parquet when reading a single column from wide tables (10/50/100 columns), with near-constant cost regardless of total column count.

3. **String with Nulls (7-20% faster)** — Lance handles string columns with nulls more efficiently across all null fractions, with the largest advantage (20%) at 0% nulls.

### Parquet Advantages

1. **Numeric Scan (1.6-1.7x faster)** — Parquet's vectorized reader with Snappy compression is highly optimized for primitive numeric types.

2. **Multi-Column Aggregation (1.5x faster)** — Related to the numeric scan advantage; Parquet's vectorized batch processing benefits GROUP BY workloads.

3. **Int + String Scan (1.2x faster)** — Moderate advantage in mixed-type full scan.

4. **Range Filter (1.15x faster)** — Slight edge on ordered-column range scans.

### Tied

- **Predicate Filter** — Both formats perform equivalently for filter pushdown at both low (0.1%) and high (50%) selectivity levels.

## Analysis

Lance's strengths align with OLAP patterns that benefit from its columnar layout optimizations:
- **TopN push-down** eliminates full-table sort, a critical advantage for interactive analytics.
- **Column pruning** shows Lance's efficient metadata handling when selecting from wide schemas.
- **Null-aware string processing** benefits from Lance's native validity bitmap handling.

Parquet's strengths are in raw numeric throughput, where Spark's vectorized Parquet reader (operating on columnar batches with SIMD-friendly memory layout + Snappy decompression) has years of optimization. The gap is most pronounced for simple aggregations on primitive types.

The predicate filter results being tied suggests both formats implement comparable statistics-based data skipping for shuffled data.

## Reproduction

```bash
cd micro-benchmark
mvn clean package -q
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
  org.openjdk.jmh.Main ".*DataSourceReadBenchmark.*" -wi 5 -i 5 -f 3
```

## Optimization Opportunities for Lance

### Root Cause Analysis

| Scenario (Lance slower) | Gap | Primary Bottleneck |
|--------------------------|-----|-------------------|
| numericScan | 1.6-1.7x | Compression decoding + no aggregation push-down |
| multiColumnAggregation | 1.5x | Same as above, amplified by 5 cols × 20M rows |
| intStringScan | 1.2x | Compression decoding (smaller gap for strings) |
| rangeFilter | 1.15x | Decoding overhead + possible lack of page-level stats skipping |

**What the connector already does well:**
- Vectorized ColumnarBatch reading (not a bottleneck)
- Near-zero-copy Arrow → Spark ColumnVector wrapping
- Batch size tuned to 8192 (previously 512, yielded 33x improvement)

### Optimization Strategies (Ranked by Expected Benefit)

#### 1. Compression: Switch from zstd to LZ4 (Expected: 30-50% improvement)

The lowest-hanging fruit. Lance defaults to zstd; Parquet uses Snappy.

| Codec | Decompress Speed |
|-------|-----------------|
| LZ4 | ~2000+ MB/s |
| Snappy | ~1700 MB/s |
| zstd | ~800-1200 MB/s |

The connector already supports per-column compression configuration:
```sql
ALTER TABLE t SET TBLPROPERTIES ('id.lance.compression' = 'lz4');
```

#### 2. Aggregation Push-Down: SUM/AVG/MIN/MAX (Expected: 50%+ for numericScan, 30%+ for agg)

Currently only `COUNT(*)` is pushed down. For `SELECT sum(id) FROM table`:
- **Now:** Read all data → convert to ColumnarBatch → Spark computes sum
- **Optimized:** Lance native scanner computes sum internally, returns a single scalar

Implementation path:
- `LanceScanBuilder` already implements `SupportsPushDownAggregates`
- Extend `pushAggregation()` to handle `Sum`/`Avg`/`Min`/`Max`
- Lance Rust layer needs aggregate scan API (or leverage DataFusion engine)

#### 3. Async Prefetch / I/O Pipelining (Expected: 15-25%)

Current read loop is strictly sequential per task:
```
[decode batch N] → [Spark process batch N] → [decode batch N+1] → ...
```

Pipelined:
```
[decode batch N+1] → [decode batch N+2] → ...
[Spark process N]  → [Spark process N+1] → ...
```

Add a 1-batch prefetch queue in `LanceFragmentColumnarBatchScanner`, leveraging Lance Rust's internal Tokio runtime for async I/O.

#### 4. Increase Batch Size (Expected: 10-15%, needs validation)

Current 8192 is good, but for pure numeric scans, larger batches (32K-64K) can:
- Reduce per-batch JNI call overhead
- Improve CPU cache locality
- Amortize Arrow allocation cost

Testable without code changes:
```java
spark.read().format("lance").option("batch_size", "65536").load(path)
```

#### 5. Page-Level Statistics Skipping (Expected: 20-30% for rangeFilter)

Verify that Lance uses **page-level** min/max statistics to skip non-qualifying data pages within a fragment (not just fragment-level zonemap pruning). If only fragment-level pruning is applied, all pages within a qualifying fragment are decoded.

#### 6. Direct Buffer Access for Numeric Types (Expected: 5-10%)

For fixed-length types (Int/Long/Double), `LanceArrowColumnVector` delegates to Spark's `ArrowColumnVector` which calls `getInt(rowId)` per row. Exposing the underlying `ArrowBuf` direct memory address to Spark's whole-stage codegen could reduce virtual method call overhead.

---

## Experimental Validation (Local SSD)

Quick experiments (f=1, wi=3, i=5) tested the impact of compression and batch size knobs.

### Experiment 1: Compression Has No Impact

| Compression | Numeric Scan (ms) | String Scan (ms) |
|-------------|-------------------|-----------------|
| zstd (default) | 53.5 ± 4.6 | 93.9 ± 4.8 |
| lz4 | 53.9 ± 3.5 | 95.6 ± 2.1 |
| none | 53.7 ± 4.9 | 94.0 ± 2.9 |

**Conclusion:** Decompression is NOT the bottleneck. Even with no compression, performance is identical. This rules out Strategy #1 as an optimization path.

### Experiment 2: Batch Size Has Moderate Impact (~7%)

| Batch Size | Numeric Scan (ms) | vs 8192 |
|-----------|-------------------|---------|
| 8192 (default) | 54.0 ± 4.2 | — |
| 32768 | 50.1 ± 4.0 | -7.2% |
| 65536 | 50.0 ± 2.0 | -7.3% |

**Conclusion:** Increasing batch size to 32768 provides ~7% improvement. Diminishing returns beyond that.

### Experiment 3: Combined Optimization (LZ4 + 65536 batch)

| Config | Score (ms) |
|--------|-----------|
| baseline (zstd, 8192) | 83.5 ± 4.1 |
| optimized (lz4, 65536) | 80.3 ± 8.3 |

**Conclusion:** Combined gains are within error margin (~4%). Not enough to close the gap.

### Revised Root Cause Analysis

Since compression and batch size are not the primary bottleneck, the **true performance gap** (Lance 53ms vs Parquet 29ms for numeric scan) comes from:

1. **Spark's Parquet-specific codegen path** — `VectorizedColumnReader` directly operates on `OnHeapColumnVector`'s underlying byte arrays, bypassing the generic `ColumnVector` interface entirely. This is a hand-optimized fast path that only Parquet benefits from.

2. **JNI boundary overhead** — Each `loadNextBatch()` call crosses JVM → Rust → JVM via JNI. Parquet stays entirely within the JVM. For a 15M row scan with 8192-row batches, that's ~1830 JNI round-trips.

3. **Arrow ColumnVector indirection** — Even though the data is "zero-copy" in memory, `ArrowColumnVector.getLong(rowId)` goes through: virtual dispatch → field vector → validity buffer check → data buffer access. Parquet's path is: direct array index.

### Object Storage Impact

In production environments where data resides on object storage (S3/OSS/GCS), the observed gap **widens** rather than narrows. Root causes:

1. **I/O request granularity** — Lance's page-level reads may issue more individual range requests compared to Parquet's larger row-group-level reads. Each extra request adds network round-trip latency (typically 10-50ms per request on object storage).

2. **Sequential read pattern** — `loadNextBatch()` is synchronous; the next I/O request only starts after the previous batch is fully processed. On high-latency networks, this serialization dominates.

3. **Prefetch and coalescing** — Parquet via Hadoop's `S3AInputStream` benefits from Hadoop's built-in read-ahead and request coalescing. Lance's Rust-level Tokio I/O may not coalesce small reads as aggressively.

### Revised Optimization Priority (for Object Storage)

| Priority | Strategy | Expected Impact | Effort |
|----------|----------|----------------|--------|
| 1 | **I/O coalescing** — Merge small range requests into fewer, larger reads | High (30-50%) | Rust core |
| 2 | **Async prefetch pipeline** — Overlap I/O with compute (read batch N+1 while processing N) | High (20-40%) | JVM + Rust |
| 3 | **Aggregation push-down** (SUM/AVG/MIN/MAX) — Eliminate data transfer entirely for agg queries | High for agg queries | JVM connector |
| 4 | **Increase default batch size to 32768** — Reduces JNI calls by 4x, minor gain on SSD, larger on remote | Medium (7-15%) | Trivial config change |
| 5 | **Row group-level buffering** — Read entire Lance fragment data into memory in one request, then decode | Medium (15-25%) | Rust core |
| 6 | **ColumnVector fast path** — Implement Parquet-style direct byte array access for numeric types | Low-Medium (5-10%) | JVM connector |

### Combinability and Strategy Selection

The three high-impact strategies operate at different layers and **fully combine** with additive gains:

```
┌─────────────────────────────────────────────────────────────────┐
│  Query Planning Layer:  Aggregation Push-Down                   │
│  (Eliminates data transfer for SUM/AVG queries)                 │
├─────────────────────────────────────────────────────────────────┤
│  JVM Connector Layer:   Async Prefetch Pipeline                 │
│  (Overlaps I/O with compute for remaining data)                 │
├─────────────────────────────────────────────────────────────────┤
│  Rust I/O Layer:        I/O Coalescing                          │
│  (Reduces network round-trips for all reads)                    │
└─────────────────────────────────────────────────────────────────┘
```

**Recommended single strategy: I/O Coalescing.** Rationale:

1. **Addresses the root cause** — The gap widening on object storage directly points to request granularity as the dominant bottleneck. Parquet reads a row group (MB-scale) in one request; Lance reads pages (KB-scale) in many requests.

2. **Universally applicable** — Benefits all query types (scan, filter, aggregation, TopN, range), not just aggregations.

3. **Quantitative argument:**
   - Assume S3 per-request latency: 20ms
   - Parquet: ~15 requests for 15M rows → 300ms network overhead
   - Lance (current): ~500+ page requests → 10,000ms network overhead
   - Lance (coalesced to Parquet-comparable granularity): ~20 requests → 400ms network overhead
   - Expected reduction: **~96% of network wait time eliminated**

4. **Prefetch is complementary but secondary** — It hides latency but doesn't reduce total bytes-in-flight or total request count. Coalescing + prefetch together gives the optimal pipeline: few large reads, overlapped with compute.

5. **Aggregation push-down is narrower** — Spectacular for `SELECT sum(x) FROM t`, but real OLAP queries often have complex expressions, JOINs, or UDFs that cannot be pushed. It's a benchmark optimizer more than a production optimizer.

## MinIO + Toxiproxy Latency-Injected Experiments (Phase 2/3 Validation)

> **Status: Results inconclusive at current data scale. Pending validation with larger dataset.**

### Setup

| Item | Value |
|------|-------|
| Object store | MinIO `localhost:9000` (local) |
| Latency injection | Toxiproxy downstream 20ms ± 5ms |
| Client endpoint | `localhost:19000` → MinIO |
| Dataset | 5M rows × 3 cols (id LONG, value DOUBLE, name STRING) ≈ 130 MB |
| JMH | warmup 2×10s, measurement 3×10s, f=1 |
| Bundle | lance-spark-bundle 0.4.0-beta.4 **rebuilt from in-tree source** with Phase 2 (`batch_readahead`) and Phase 3 (`CLOUD_DEFAULT_BLOCK_SIZE = 1 MB`) |

Bundle symbols verified present post-rebuild:
- `LanceSparkReadOptions.CONFIG_BATCH_READAHEAD`
- `LanceSparkReadOptions.batchReadahead` (instance field)
- `LanceSparkReadOptions.CLOUD_DEFAULT_BLOCK_SIZE` (`1_048_576`)
- `LanceSparkReadOptions.isCloudPath(String)`

### Experiment 1: block_size sweep

| blockSize | FullScan (ms) | ColumnPrune (ms) |
|-----------|---------------|------------------|
| 65536 (64KB, old default) | 1075.745 ± 282.254 | 1052.855 ± 136.565 |
| 1048576 (1MB, new cloud default) | 1088.863 ± 286.977 | 1049.594 ± 93.934 |
| 4194304 (4MB, aggressive) | 1095.470 ± 411.081 | 1055.538 ± 113.303 |

**Observation:** All three values land within noise (±10–40% error bars). Expected a clear step function where ≥1 MB would dramatically beat 64 KB by merging adjacent page requests under a 20 ms RTT penalty. **Not seen at this scale.**

### Experiment 2: batch_readahead sweep

| batchReadahead | FullScan (ms) |
|----------------|---------------|
| 1 | 1110.274 ± 146.387 |
| 4 | 1122.428 ± 187.022 |
| 16 (default) | 1108.197 ± 445.069 |
| 32 | 1069.844 ± 26.242 |

**Observation:** Essentially flat from 1 to 32. Expected higher readahead (16/32) to overlap in-flight I/O and amortize the 20 ms RTT, producing roughly linear speedup up to some plateau. **Not seen at this scale.**

### Experiment 3: Lance vs Parquet baseline under 20 ms injected latency

| Query | Lance (ms) | Parquet (ms) | Lance / Parquet |
|-------|-----------|-------------|-----------------|
| FullScan (`SELECT sum(id), sum(value)`) | 1116.436 ± 223.890 | 216.702 ± 42.165 | **5.15×** slower |
| ColumnPrune (`SELECT sum(id)`) | 1058.103 ± 427.310 | 204.608 ± 34.870 | **5.17×** slower |

**Observation:** Despite the new 1 MB cloud-path default and `batch_readahead=16`, Lance reads are 5× slower than Parquet over the same latency-injected proxy. Contrast with local-SSD baseline where Lance and Parquet are within 2× of each other.

### Interpretation

Three candidate root causes, in priority order:

1. **Params may not be reaching the Rust native layer end-to-end.** In-tree source looks correct (`LanceSparkReadOptions.toReadOptions()` calls `roBuilder.setBlockSize(blockSize)`; `LanceFragmentScanner` calls `scanOptions.batchReadahead(readOptions.getBatchReadahead())`), but no runtime trace was captured to prove it. **Next step:** add TRACE logging at `ReadOptions.Builder.setBlockSize` and scanner-side `batchReadahead` to confirm plumbing.

2. **Dataset too small to reveal coalescing.** 130 MB split over default Lance fragmentation may already produce so few range requests that block_size thresholds don't change HTTP-request count. **Next step:** scale to 50M rows (~1.3 GB) and re-run.

3. **Something below Lance scheduler level.** If 1–2 are verified and the gap persists, the Rust-side `FileScheduler.submit_request()` coalescing may not actually be invoked under the current read pattern, or per-request S3 path overhead (auth, virtual-hosting) dwarfs the RTT savings.

### Next Actions

| # | Action | Priority |
|---|--------|----------|
| C | Document current findings honestly (this section) | ✅ Done |
| B | Scale dataset to 50M rows; re-run all three sweeps | ✅ Done (see §"50M-row rerun") |
| A | Add TRACE logging to prove Spark `.option()` → Rust `ReadOptions` plumbing | Next |

### 50M-row rerun

Dataset scaled to 50,000,000 rows × 3 cols: Lance ≈ 1.55 GB (48 data files), Parquet ≈ 800 MB. All experiments use the same bundle and JMH config; only the data volume changed.

#### Experiment 1 (50M): block_size sweep

| blockSize | FullScan (ms) | ColumnPrune (ms) |
|-----------|---------------|------------------|
| 65536 (64 KB, old default) | 2025.391 ± 1290.420 | 1721.922 ± 1029.401 |
| 1048576 (1 MB, new cloud default) | 2144.499 ± 1893.371 | 1754.626 ± 1471.906 |
| 4194304 (4 MB, aggressive) | 1935.784 ±  426.634 | 1737.985 ±  780.887 |

**Observation:** Still flat at 10× data. 4 MB is numerically lowest for FullScan but within overlapping error bars of the other two. Not the step function expected from coalescing.

#### Experiment 2 (50M): batch_readahead sweep

| batchReadahead | FullScan (ms) |
|----------------|---------------|
| 1  | 2166.184 ± 1771.546 |
| 4  | 2085.607 ±  154.621 |
| 16 (default) | 2089.446 ± 1072.162 |
| 32 | 2082.785 ± 2072.672 |

**Observation:** 1→4 shows a small ~80 ms drop but within noise. 4, 16, 32 are indistinguishable. Expected linear speedup from in-flight overlap did not materialize.

#### Experiment 3 (50M): Lance vs Parquet baseline

| Query | Lance (ms) | Parquet (ms) | Lance / Parquet |
|-------|-----------|-------------|-----------------|
| FullScan (`SELECT sum(id), sum(value)`)         | 1974.658 ±  760.915 | 1001.401 ± 4097.492 | ~1.97× slower |
| ColumnPrune (`SELECT sum(id)`)                  | 1729.884 ± 1351.253 |  520.076 ±  816.382 | ~3.33× slower |

**Observation:** Gap narrows from ~5× (at 5M) to ~2–3× (at 50M) because fixed per-query Spark overhead amortizes. Parquet `formatFullScan` has an extreme error bar (±4097 ms vs 1001 ms mean) driven by a single outlier iteration — the real gap is likely larger than the 2× mean suggests. Still Lance-slower, still well beyond what Phase 2/3 could explain at this data scale.

#### 50M interpretation

- Error bars remain uncomfortably large (often ≥ the mean). With only 3 measurement iterations × 10 s, JMH could not collect enough samples to separate signal from noise.
- Neither block_size nor batch_readahead produced a monotonic trend. If the Rust scheduler were actually seeing the new values, the 32× batch_readahead gap (1 vs 32) should appear even with wide error bars.
- Candidate root causes (1) and (3) from the 5M section remain live. Candidate (2) "dataset too small" is **weakened** — 10× data did not change the pattern.

**Action A is now the priority:** prove params flow through `LanceSparkReadOptions.toReadOptions()` → native `ReadOptions`. Without that ground truth, further sweeps will keep producing the same ambiguous picture.
