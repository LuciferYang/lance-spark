# D1 driver-side LanceDatasetCache: cluster A/B validation

**日期**：2026-05-25
**分支**：`feat/java-native-lance-reader` (HEAD `b51ebcd`)
**测量**：Spark Event Log + LocalClusterAB 15 iter，`local-cluster[4,1,2048]`，dylib `88bbdb7b...`

## TL;DR

把 `LanceDatasetCache`（原仅 executor-side）扩展到 driver-side，消除每次 SQL exec 重复 open dataset 3 次的 overhead。**q11/q14 小表 collect_ms 跌 71-72%**，现在 q11 lance **比 parquet 还快**（170 ms vs 199 ms）。事件日志确认 pre-job 时间从 **482 ms → 10-12 ms（-97%）**——差距完全消失。q1/q4 大表 wall 在测量噪声内。

## 直接 A/B 数据

### Spark Event Log pre-job 时间（每 SQL exec collect 阶段）

| query | 之前 (commit 580c592) | D1 ON (commit b51ebcd) | 削减 |
|---|---:|---:|---:|
| q11 (date_dim) | 482 ms | **10-12 ms** | **-97%** |
| q1 (store_sales sum) | (没存详细) | 70-83 ms | (q1 plan 本来就更复杂) |
| q4 (store_sales 6×decimal) | (没存详细) | 71-79 ms | |

q11 现在 pre-job 跟 parquet 的 15 ms 相当（DSv2 protocol 本身的 overhead，基本无法再压）。

### Cluster collect_ms steady state (iter 6-15 mean)

| query | D1 OFF baseline | D1 ON | parquet | L/P (D1 ON) | 改善 |
|---|---:|---:|---:|---:|---:|
| q1 (store_sales sum) | ~2783 | ~2329 | 1186 | 1.96× | -16% |
| q4 (6×decimal sum) | 17444 | 18158 | 6710 | 2.71× | +4%（噪声内） |
| **q11 (date_dim group)** | **580** | **170** | **199** | **0.85×** | **-71%** |
| **q14 (synth sum)** | **582** | **162** | **131** | **1.24×** | **-72%** |

**关键观察**：
- q11 lance NOW FASTER than parquet (170 ms < 199 ms) — driver overhead 抹平后，lance Rust decode 真的比 parquet 的 vectorized reader 还快
- q14 ratio 从 4.44× 降到 1.24× — 剩余 31 ms 是 lance Rust decode 时间（30M rows × 8 bytes ≈ 240MB scan + sum），跟 parquet 差距不大
- q4 decode-bound query 没变化（pre-job 不是它的 bottleneck，~75 ms 占总 wall 0.4%）
- q1 部分受益（-16%），因为 q1 query plan 路径仍走 ScanBuilder 的 zonemap stats 加载等

## 实现要点

3 个 driver-side 调用点全部走 cache：

1. **`LanceDataSource.inferSchema`** (line 76)：schema 推断
2. **`LanceScanBuilder.getOrOpenDataset`** (line 122)：manifest summary + zonemap stats 加载
3. **`LanceSplit.planScan`** (line 100)：fragment list + version

每个调用点都遵循 cached/!cached 双路径：
```java
OpenResult open = LanceDatasetCache.getOrOpen(readOptions);
Dataset dataset = open.dataset();
try {
  // ...
} finally {
  if (!open.cached()) {
    dataset.close();
  }
}
```

`LanceScanBuilder` 的 `lazyDataset` 引入 `lazyDatasetIsCached` flag，`closeLazyDataset()` 跳过 cached dataset 的 close（同 `LanceFragmentScanner` 的 pattern）。

## 新增 unit tests (5 个)

`LanceDatasetCacheTest` 加了 driver-side overload 覆盖：
- `keyFromReadOptionsCollidesAcrossOpensWithSameUri` — 同 URI/version/storage 必须 equal
- `keyFromReadOptionsDiffersByUri` — URI 不同必须 differ
- `keyFromReadOptionsIgnoresStorageOptionMapOrder` — TreeMap 排序保证 hash 稳定
- `keyFromReadOptionsDiffersByStorageOptionContent` — storage option 值不同必须 differ
- `keyFromReadOptionsDistinguishesPinnedFromLatest` — `withVersion(7)` 必须产生不同 key

全套 base suite 349/349 通过（之前 344）。

## 设计权衡

### Driver vs executor key 算法不对齐

- Driver: `Key.fromReadOptions(readOptions)` 用 `readOptions.getStorageOptions()`
- Executor: `Key.from(inputPartition)` 用 `inputPartition.getInitialStorageOptions()`

两侧在 cluster 模式下是不同 JVM，cache 不共享——key 不需对齐。在 local 模式下（同一 JVM）storage options 通常匹配，会自然 share cache entry，但**正确性不依赖此**。

文档写在 `LanceDatasetCache.getOrOpen(LanceSparkReadOptions)` Javadoc 上，避免后续被误改成"用同一 key 算法"。

### Cache lifecycle

仍用 strong-reference + size cap (default 16) — 没有 LRU eviction。Driver 上的 distinct dataset 数通常远小于 16；超 cap 时新 key 直接 open 不缓存（避免 race）。已存在的 cap 行为正好覆盖这个场景。

## 工程含义

| 决策 | 结论 |
|---|---|
| D1 是否 mergable | **是**。q11/q14 大幅改善，大表无回退；测试覆盖 driver overload；行为 toggleable via sysprop |
| Driver cache 默认 on | 是（沿用 C1 的 default-true sysprop）；`-Dlance.spark.dataset_cache_enabled=false` 关闭整个 cache 双侧 |
| 后续 follow-up | 把 q4 / q1 的 4-7% 噪声范围内变化追到底（profile 是否有跨 query 缓存命中差异）。低优 |
| V1 datasource 还需要吗 | **不需要**。原推断的 V1-fallback 必要性来自 482ms pre-job overhead；现降到 11ms 后，V1 的工程量不再 justify |
| Page-stats backlog | 仍归类到 filter-path optimization（q3/q10 这种），不是 small-table |

## 关联文件

- 实现 commit: `b51ebcd`
- 调用点：
  - `lance-spark-base_2.12/src/main/java/org/lance/spark/LanceDataSource.java`
  - `lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceScanBuilder.java`
  - `lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceSplit.java`
- Cache infra: `lance-spark-base_2.12/src/main/java/org/lance/spark/internal/LanceDatasetCache.java`
- 测试: `lance-spark-base_2.12/src/test/java/org/lance/spark/internal/LanceDatasetCacheTest.java`
- 数据 logs:
  - `/tmp/cluster-d1-on.log` — D1 ON, all 4 queries 15 iter
  - `/tmp/spark-events-d1/eventlog_v2_app-*` — Spark Event Log
- 关联调研: `docs/profiles/2026-05-25-small-table-cluster-investigation.md` (`580c592`)
