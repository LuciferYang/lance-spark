# q4 cluster flamegraph 分析：修正之前的推断

**日期**：2026-05-25
**分支**：`feat/java-native-lance-reader` (HEAD `b51ebcd`)
**采样**：async-profiler 4.4 wall mode interval=2ms attached via `-agentpath` to 4 executor JVMs in local-cluster[4,1,2048] mode, 8 iter q4 only, JFR 输出
**dylib**：lance-jni `88bbdb7b...` (PR#1 + PR#6858 都包含)

## TL;DR

之前的减法估计 (`task_runtime - probe_internal_scan = "Spark side"`) 是错的。Probe 测的是 `loadNextBatch()` 从 JNI 视角的 wall，但 lance tokio runtime 把 IO+decode 都重叠在 `block_on` 期间——probe 严重低估了 lance Rust 实际 CPU。火焰图直接看 task threads + tokio worker threads 的 wall 分布，得出真实归因：

| 来源 | 占比 | 详情 |
|---|---:|---|
| **lance Rust decode** | **~60%** | bitpacking unpack (16_1, 32_14/15/21) + validity (BooleanBufferBuilder) + memmove |
| Spark agg codegen | ~25% | MathUtils.addExact via LambdaForm + hashAgg_sum_0..5 stubs + Decimal.longVal_$eq |
| 其他 (idle, IO wait, JVM) | ~15% | |

## Task-thread wall 分布（按 leaf frame 累计）

```
Task-thread total: 119,481 wall samples
  53.6% __psynch_cvwait
        └─ 96.7% via tokio::loom::std::parking_lot::Condvar::wait
        └─ 解读：task 线程 block_on(future) 阻塞，等 tokio 后台 worker 完成 IO+decode
  16.7% LambdaForm$DMH.newInvokeSpecial   ← Spark codegen MathUtils.addExact
   3.1% hashAgg_doAggregate_sum_0..5      ← Spark codegen stubs
   2.4% LanceArrowColumnVector.isNullAt
   1.1% Decimal.longVal_$eq
   0.5% BoxesRunTime.unboxToLong
```

## Tokio worker thread wall 分布（这才是真实 CPU 工作）

```
12.3% lance_bitpacking::unpack_16_1        ← width-1 unpack (validity bits)
 7.4% _platform_memmove
 4.4% lance_bitpacking::unpack_32_14       ← decimal width=14 (NarrowU32 path)
 4.4% lance_bitpacking::unpack_16_1::mask
 4.1% slice::iter::find_map
 3.9% lance_bitpacking::unpack_32_21       ← decimal width=21 (NarrowU32 path)
 2.9% BooleanBufferBuilder::advance        ← null bitmap construction
 2.6% FixedWidthDataBlockBuilder::append   ← materialize decoded batches
 2.3% lance_bitpacking::unpack_32_15
 2.1% DictionaryDataBlock::decode_helper
 1.9% bit_util::set_bit_raw
 1.9% slice index
 1.9% repdef::RepDefUnraveler::unravel_validity
 ...
```

**bitpacking unpack accumulated ≈ 26%**（width-specific kernels）  
**memcpy + memmove ≈ 9%**  
**validity bitmap construction ≈ 7%**  
**iteration / slice index ≈ 10%**  
**materialization (DataBlockBuilder.append) ≈ 5%**

## 校正后归因 (估)

q4 cluster wall 17500 ms 中：
- ~60% (~10500 ms) lance Rust decode (bitpacking + validity + materialization)
- ~25% (~4400 ms) Spark agg (MathUtils.addExact 6 ×, decimal box/unbox)
- ~15% (~2600 ms) idle / IO wait / JVM overhead

## 重新评估优化方向

| 方案 | 预期收益 | 工程量 | 备注 |
|---|---:|---|---|
| Java-side SUM pushdown | **~25% (q4 18s → 13.5s)** | 1-2d | 只能省 Spark agg 部分；lance decode 仍要做 |
| lance Rust scalar agg pushdown (真下推) | **~75% (q4 18s → 4-5s)** | 1-2w | 在 lance Rust 里算 sum，跳过 materialization。利用 DataFusion infra |
| 调大 batch_size (8192 → 1M) | ~5-10% | <1h | 减 FFI/per-batch overhead |
| AArch64 NEON SIMD bitpacking | ~10-20% lance decode | weeks | lance Rust 改动；硬件相关 |

**之前推断的 "Java pushdown 能让 q4 → 2-3s" 是错的**。Java pushdown 只能节省 Spark agg 这 25%。要真正让 q4 接近 parquet 6710ms，**必须有 lance Rust scalar agg pushdown**——把 sum 在 lance 里完成，避免 1.89M rows × 6 cols 的 decimal128 实化到 Arrow buffer。

## 为什么 parquet decode 比 lance 快 3×？

Parquet 在 Spark 走 native `VectorizedColumnReader`（Java 层、off-heap），针对 parquet PLAIN/RLE_DICTIONARY 编码做了手工 SIMD 优化。
Lance bitpacking 用 fastlanes-style portable Rust，没有 AArch64 的 NEON 手写优化。
这是体系结构差距，不是 lance 的实现 bug。

## 关联文件

- 火焰图 HTML: `/tmp/q4-flamegraph/exec-{72210,72211,72212,72213}.html`
- 原始 JFR: `/tmp/q4-flamegraph/exec-*.jfr` (~26 MB each)
- collapsed stacks: `/tmp/q4-flamegraph/exec-72210.collapsed`
- 测量 setup：`micro-benchmark/.../LocalClusterAB.java` 加 `-Dlance.spark.profile_agentpath=`，executor JVM 启动 attach async-profiler agent
- 之前推断 (已 superseded): `docs/profiles/2026-05-25-small-table-cluster-investigation.md` 提到 "q4 90% Spark agg" — 现修正

## 推荐下一步

1. **Java SUM pushdown** 仍然有价值（25% 收益，2 天工程量）—— 是 lance-spark 层面能做的最大优化
2. **lance Rust scalar agg pushdown** 是 longer-term backlog，需要新一轮设计 + JNI 接口
3. q1 的瓶颈类似（lance Rust int32 decode 占大头），但绝对值小（2.3s vs parquet 1.2s, 仅 2× 差距），ROI 不如 q4

如果只能挑一个：**先做 Java SUM pushdown** —— 工程量可控，q4 降 25%，q1 也受益。
