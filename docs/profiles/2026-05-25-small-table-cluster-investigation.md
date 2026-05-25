# 小表 cluster 落后调研：Spark DSv2 driver-side overhead

**日期**：2026-05-25
**分支**：`feat/java-native-lance-reader` (HEAD `1afd1fa`)
**测量**：Spark Event Log + per-task probe，`local-cluster[4,1,2048]`，iter 8 稳态

## TL;DR

q11/q14 cluster 落后 parquet 3-4× 的根因 **NOT** lance 内部 decode（B3 docs 旧判断 "page-stats 缺失" 是错的）。**95% 的 wall time 在 lance 之外的 Spark DSv2 driver path**——每次 SQL exec 都重新 open 几次 dataset。Fix candidate 是 driver-side dataset cache（C1 cache 的扩展），预计能省掉小表 50-70% 的 wall。

## 直接 A/B（同一 q11 query，iter 8 稳态）

| 阶段 | parquet | lance | 差距 |
|---|---:|---:|---:|
| **Total SQL wall** | **179 ms** | **637 ms** | +458 ms |
| **pre-job time** (Catalyst + DSv2 plan) | **15 ms** | **482 ms** | **+467 ms** |
| Job wall (实际 stage 执行) | 162 ms | 153 ms | -9 ms (lance 略快) |
| post-job (result IPC) | 2 ms | ~2 ms | ~0 |

**Internal lance scan 只用了 28 ms**（probe `[lance-fixed-cost]`），剩下 482 ms pre-job + 153 ms stages = 635 ms。差距全在 pre-job。

## Pre-job 482ms 拆解（驱动器端 dataset open 重复发生）

抓到 lance-spark 在 driver 上至少 **3 次 dataset open** 每次 SQL exec：

| 调用点 | 文件 / 行号 | 用途 |
|---|---|---|
| 1 | `LanceDataSource.inferSchema()` line 76 | schema 推断（注册 view 时 + 部分 SQL exec 时） |
| 2 | `LanceScanBuilder.getOrOpenDataset()` line 122 | 统计 / manifest summary / zonemap stats |
| 3 | `LanceSplit.planScan()` | fragment 列表 + version + sizes |

每次 open 即使 ObjectStoreRegistry 是热的（PR#1 落地后），仍要：
- 解析 manifest（S3 GetObject + 反序列化）
- 创建新 `Dataset` 实例 + Session
- ~30-100 ms / open

3 次 × 60 ms = 180-300 ms 仅 dataset open；剩下 ~200ms 是 Spark 自己的 catalyst optimize + 物理 plan + DSv2 ScanBuilder 协议 (pushdown、partitioning 报告)。

## 探针数据（`-Dlance.spark.probe_fixed_cost=true`）

q11 date_dim (1 fragment, 73K rows, n=24 task-fragment runs):
- open mean = 24.1 ms (max 146 cold)
- first_batch = 4.0 ms
- subsequent = 0.0 ms (over 9 batches)
- **TOTAL internal = 28.1 ms**

q14 numeric_bp128 (32 frags → 10 partitions with coalesce, n=768 runs):
- open mean = 1.5 ms (max 295 cold)
- first_batch = 6.1 ms
- subsequent = 5.0 ms (over 114 batches)
- **TOTAL internal per-fragment-scan = 12.6 ms**
- 单 task 实际工作 ≈ 3-4 fragments × 12.6 = 50ms decode

q11 wall 580ms - 28ms internal = 552 ms 全在 Spark + driver  
q14 wall 582ms - ~150ms decode (with 4-way parallelism) = ~430 ms 全在 Spark + driver

## Fragment 布局（Fragment Size Probe）

| dataset | fragments | size | rows |
|---|---:|---:|---:|
| date_dim_bp128.lance | 1 | 4.5 MB | 73,049 |
| synth/numeric_bp128.lance | 32 | 1,297 MB | 30,000,000 |
| store_sales_bp128_dispatch.lance | 305 | 17,043 MB | 287,997,024 |

q11 已经只有 1 fragment，coalesce 帮不上。q14 已经 coalesce 到 10 partitions，但 wall 仍 ~582ms。

## Fix candidates 排序（按 ROI）

### 1. **Driver-side LanceDatasetCache 扩展** ⭐⭐⭐ — high ROI, low effort
- 把 `LanceDatasetCache.getOrOpen()`（现 executor-only）拓展为 driver-also。
- 改 3 个调用点：`LanceDataSource.inferSchema`、`LanceScanBuilder.getOrOpenDataset`、`LanceSplit.planScan`。
- 预期：q11/q14 saving 200-300 ms/query → cluster L/P 从 2.91× → ~1.4-1.6×
- 风险：需要确保 cache key 正确（include version pin）+ cache 在 lifecycle 末端可关闭
- 工程量：1-2h（C1 cache 已有 infra）

### 2. **DSv2 ScanBuilder 协议精简** ⭐⭐ — medium ROI, medium effort
- DSv2 在 plan 阶段反复回调 ScanBuilder（pushFilters、buildForBatch、reportStatistics）。每次回调都可能触发 dataset 操作。
- Profile 一下 ScanBuilder 内部哪些方法在 plan 阶段被调用多次，把昂贵的 zonemap stats 加载等延迟到 `planInputPartitions()`。
- 工程量：3-5h
- 预期：再省 100-200 ms/query

### 3. **V1 datasource POC** ⭐ — high ROI, HIGH effort
- 实现 lance 的 V1 file-source（继承 `FileFormat`），让 spark 走 `FileSourceScanExec` 路径，跟 parquet 一样。
- 预期：q11/q14 几乎追平 parquet
- 缺点：需要重写 datasource 全套（read/write/predicate pushdown/codegen），weeks 量级；后续需要维护两套 datasource
- 长期 backlog，目前 ROI 不够

### 4. **Lance Rust page-level stats** — 不在小表 backlog
- 原 B3 docs 的 hypothesis。这次调研证明 q11/q14 全表扫描，没有 filter，page-stats 也帮不上 90% 的 wall。
- page-stats 主要价值在带 filter 的查询（q3/q10 这种），不是 q11/q14。
- 重新归类到 filter-path optimization backlog。

### 5. **q14 decode 速度本身慢**（次要）
- q14 在 32 fragments × 12.6ms internal × 4 cores parallel ≈ 100ms decode work，相对 wall 580ms 占比 17%。
- 即使 driver overhead 全部消失，q14 lance 仍会比 parquet 慢 ~50ms。
- 这层是 lance Rust decode 速度，跟 q4 同根因，等 1C dest-buffer / 矢量化优化。

## 建议下一步

直接做 candidate #1（driver-side cache）。已落地 C1 cache 的 infra（`LanceDatasetCache.java`），改动只需要：

1. 把 cache 的 toggle 范围从 executor-only 拓展到 driver
2. 改 `LanceDataSource.inferSchema` / `LanceScanBuilder.getOrOpenDataset` / `LanceSplit.planScan` 走 cache
3. cluster A/B q11/q14（预期 wall 下降 200-300ms）+ 大表 (q1/q4) 不退化

## 关联文件

- 实测 q11 lance event log: `/tmp/q11-events.json` (来源 `/tmp/spark-events/eventlog_v2_app-20260525195540-0000/`)
- 实测 q11 parquet event log: `/tmp/q11-parquet-events.json`
- probe 数据: `/Users/yangjie01/Tools/spark-4.0.2-bin-hadoop3/work/app-20260525185218-0000/0/stderr`
- baseline lance log: `/tmp/cluster-q11q14-lance.log`
- baseline parquet log: `/tmp/cluster-q11q14-parquet.log`
- C1 现有 infra: `lance-spark-base_2.12/src/main/java/org/lance/spark/internal/LanceDatasetCache.java`
- 调用点：
  - `lance-spark-base_2.12/src/main/java/org/lance/spark/LanceDataSource.java:76`
  - `lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceScanBuilder.java:122`
  - `lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceSplit.java` (planScan)
