# C1 dataset cache: local vs local-cluster validation

**日期**：2026-05-25
**分支**：`feat/java-native-lance-reader`（C1 已落 + 测试落档）
**dylib**：`e993780ba421827c604fa56da8650672f958f7d7e2db8058d4eac0fc99f744b3`
**测量**：`LocalClusterAB.java`，每个 query 在同一 SparkSession 内跑 12-20 iters，`noop write` + `count` + `collect` 三种 action 交替（避免 JMH 的重复-noop accumulation artifact），取 iter 6+ 稳态 collect 列均值
**Spark dist**: `/Users/yangjie01/Tools/spark-4.0.2-bin-hadoop3` 通过 `SPARK_HOME` + `SPARK_SCALA_VERSION=2.13` env 提供（local-cluster 模式下 worker fork executor JVM 需要）

## 4-quadrant 完整数据

|   q | 表规模 | local lance | local parquet | local L/P | clu lance ON | clu parquet | **clu L/P (cache ON)** | clu lance OFF | clu OFF/parquet |
|---|---|---|---|---|---|---|---|---|---|
| q1 | 288M int sum | 696 | 507 | 1.37× | 1426 | 1429 | **1.00×** | **17730** | **12.4×** |
| q2 | 288M 多列 int sum | 1398 | 1234 | 1.13× | 2916 | 2789 | **1.05×** | **19561** | **7.0×** |
| q3 | 288M date 范围 filter | 672 | 848 | 0.79× ← 反超 | 4935 | 1836 | 2.69× ⚠ | 18188 | 9.9× |
| q4 | 288M 6×decimal sum | 4397 | 2558 | 1.72× | 9699 | 6083 | 1.59× | **23390** | 3.8× |
| q11 | 73K date_dim groupby | 76 | 33 | 2.30× | 534 | 159 | 3.36× | 703 | 4.4× |
| q14 | 30M synth double sum | 97 | 79 | 1.23× | 569 | 126 | 4.52× | 2625 | 20.8× |

local parquet 数字来自之前的 JMH cache-on 报告（`docs/profiles/2026-05-25-type-coverage-with-c1.md`）；其余 4 列均来自 `LocalClusterAB.java` 同一 SparkSession 多次跑的稳态。

## 三个核心结论

### 1. C1 cache 在 cluster 模式下从"useful"升级为"critical"

没有 C1，cluster 模式大 fact 表 lance 比 parquet **慢 7-12 倍**：

- q1: clu lance OFF 17730 vs parquet 1429 = **12.4× 慢**
- q2: 19561 vs 2789 = **7.0× 慢**
- q3: 18188 vs 1836 = **9.9× 慢**
- q14: 2625 vs 126 = **20.8× 慢**

C1 cache ON 立刻拉回持平：q1 1.00×、q2 1.05×、q4 1.59×。**生产分布式部署没有 C1 完全不可用**。

技术解释：cluster 模式下每个 executor JVM 都是独立 LanceDatasetCache 域。234 fragment × 4 executor JVM × 每个 fragment 重开 dataset = 累计十几秒级别的额外开销。C1 的 ConcurrentHashMap.computeIfAbsent 把这部分压缩到每个 executor 一次 cold open + 全部后续 cache hit。

### 2. Cluster + C1 让 lance 与 parquet 在大 fact 表上完全持平

| q | local L/P | cluster L/P |
|---|---|---|
| q1 | 1.37× | **1.00×** ↓ |
| q2 | 1.13× | **1.05×** ↓ |
| q4 | 1.72× | 1.59× ↓ |

cluster 模式 IPC + 序列化是 lance 和 parquet **共同**付出的成本，相对差距收窄。q4 仍保持 1.59× 是 decode-bound（6 列 i128 sum），不是 fixed cost。

### 3. 两个 cluster-only 退化点

#### 3a. q3 filter+count 在 cluster 反转

| 模式 | lance | parquet | L/P |
|---|---|---|---|
| local | 672 | 848 | **0.79×** ← lance 反超 |
| cluster | 4935 | 1836 | **2.69×** ⚠ |

local 模式 lance 比 parquet 快 21%，cluster 模式反转为 lance 慢 169%。parquet 在 cluster 下从 848 → 1836 = +116%，lance 从 672 → 4935 = +634%。**lance 对 cluster IPC 的敏感性远高于 parquet**，特别是 filter pushdown 路径。

可能原因（待 probe 验证）：
- Spark V2 filter pushdown 在 cluster 下分发 filter 表达式给每个 task，lance scanner 重复编译/重新评估
- lance 在 fragment 级别没有 page-level statistics，无法剪枝；parquet 有 row-group min/max
- cluster 模式 task 数变少（4 cores vs local[*] 全核），filter 工作并行度降低

#### 3b. 小表（q11/q14）lance 3-5× 慢于 parquet

| q | clu L/P |
|---|---|
| q11 | 3.36× |
| q14 | 4.52× |

73K date_dim 1 fragment 或 30M synth 几个 fragment，cluster 下 task launch + IPC 是 200-400ms 固定开销。parquet 这边因为有 page-level 统计 + dictionary stats + 高效的 vectorized reader，能把这种小表查询完成在 100-160ms。lance 没有 page-level stats，每个 task 至少要扫一定数量的 row group。

C1 cache 只省 24-78%（每 query 169ms-2s），但绝对值上 lance 仍慢于 parquet。这是**架构层差异**，C1 cache 帮不上忙，需要其他优化方向（比如 lance 端 page-level stats / fragment-level pruning）。

## 工程含义

| 决策 | 结论 |
|---|---|
| C1 是否必须落地？ | **必须**。cluster 模式没有 C1 比 parquet 慢 7-20×；有了 C1 大 fact 表持平 |
| Q4 (decimal) 还需要 1C 吗？ | 仍有 1.59× 差距，是 decode-bound。1C lance Rust dest-buffer refactor 仍是高 ROI 候选 |
| q3 filter-path 退化 | 需要新一轮 probe（**B3 候选**）：cluster 模式 q3 火焰图，定位 filter-evaluation 是不是确实有 cluster-only 重复路径 |
| 小表 q11/q14 cluster 落后 | C1 不是答案；需要 lance 侧 page-level stats（架构 backlog） |

## 关联文件

- runner: `micro-benchmark/src/main/java/org/lance/spark/microbenchmark/LocalClusterAB.java`
- 数据 logs:
  - `/tmp/local-ab.log` — local lance
  - `/tmp/cluster-ab.log` — cluster lance C1 ON
  - `/tmp/cluster-nocache-full.log` — cluster lance C1 OFF
  - `/tmp/cluster-parquet.log` — cluster parquet
- 之前的 local 完整 JMH（含 19q parquet 基线）：`docs/profiles/2026-05-25-type-coverage-with-c1.md`
- C1 实现：commit `8b0eadc` (`LanceDatasetCache.java`)
- Spark 配置（master 可切 + executor JVM args 透传）：`MinioTpcdsBenchmark.java` (this commit)
- env 要求（local-cluster 模式）：`SPARK_HOME=<spark-4.0.2-bin-hadoop3>` + `SPARK_SCALA_VERSION=2.13`
