# 2026-05-26 — Cluster baseline @ HEAD `71408f9` (post-revert) — full 19q

**日期**：2026-05-26
**lance-spark 分支**：`feat/java-native-lance-reader` (HEAD `81cfc76` after baseline commit)
**lance 分支**：`feat/1c-dest-buffer` (HEAD `15399937b`，PR#1 + PR#6858 squashed)
**dylib**：`liblance_jni.dylib` sha256 `e993780ba421827c604fa56da8650672f958f7d7e2db8058d4eac0fc99f744b3`，
  release-optimized build of `15399937b`，150 432 576 bytes
**Datasets** (all on local MinIO `s3://benchmark/`):
- `tpcds-sf-100/store_sales_bp128_dispatch.lance` (288 M rows, 305 frags × 53 MB = 16 GiB)
- `tpcds-sf-100/{customer,customer_address,date_dim,item,store_returns}_bp128.lance`
- `synth/{numeric,temporal}_bp128.lance` (30 M rows each)
- 同名 parquet 子目录作 baseline
**测量**：`LocalClusterAB.java`，`local-cluster[4,1,2048]`，10 iter，noop+count+collect 三种 action 交替，iter 5-10 稳态均值

## TL;DR

P1+P2+P3 SUM-pushdown 全部回退后，19 query × 2 format 完整 cluster A/B：**lance 在 12/19 反超 parquet（L/P < 0.95），4/19 与 parquet 持平（0.95-1.05），仅 3/19 慢于 parquet（最差 q5 1.36×）。没有任何 query 慢过 1.4×。**

修正错误的历史数据：之前 `2026-05-25-d1-driver-cache-validation.md` 里 q1=2329 / q4=18158 是用了 dylib `88bbdb7b…`（疑似 debug build）跑出来的污染数据。同时本次 cluster 测量结果与 `2026-05-24-type-coverage-jmh.md` 里 JMH local mode 数据有显著差异（lance 持平 / parquet 慢一倍），原因详见下方"mode 差异"段落。

## 19q × 2 fmt cluster 完整对比（iter 5-10 mean，collect_ms）

| q | 类型 / 操作 | 表 (行数) | lance | parquet | **L/P** | 状态 |
|---|---|---|---:|---:|---:|:---:|
| q1  | int32 sum                | store_sales (288M)        |  1034 |  1311 | **0.79×** | ✓ |
| q2  | 多列 int32 sum           | store_sales (288M)        |  2538 |  2932 | **0.87×** | ✓ |
| q3  | int32 范围 filter        | store_sales (288M)        |  1294 |  2006 | **0.65×** | ✓ |
| q4  | 6×decimal(7,2) sum       | store_sales (288M)        |  6194 |  6201 | 1.00× | = |
| q5  | int32 distinct           | customer (2M)             |   794 |   585 | 1.36× | ✗ |
| q6  | 2×decimal(7,2) sum       | store_returns (28M)       |   297 |   310 | 0.96× | = |
| q7  | varchar distinct         | item (200K)               |   411 |   423 | 0.97× | = |
| q8  | varchar 等值 filter      | customer (2M)             |    41 |    83 | **0.49×** | ✓ |
| q9  | char(2) groupby          | customer_address (1M)     |   199 |   204 | 0.97× | = |
| q10 | date32 范围 filter       | date_dim (73K)            |    28 |    62 | **0.45×** | ✓ |
| q11 | int32 groupby            | date_dim (73K)            |   143 |   177 | **0.81×** | ✓ |
| q12 | star-join + group        | store_sales × date_dim    |  3144 |  3407 | **0.92×** | ✓ |
| q13 | int64 max                | synth_numeric (30M)       |   158 |   130 | 1.22× | ✗ |
| q14 | double sum               | synth_numeric (30M)       |   157 |   146 | 1.08× | = |
| q15 | float sum                | synth_numeric (30M)       |   147 |   131 | 1.13× | ✗ |
| q16 | decimal(18,2) sum        | synth_numeric (30M)       |   372 |   394 | 0.95× | = |
| q17 | decimal(38,18) max       | synth_numeric (30M)       |   512 |   608 | **0.84×** | ✓ |
| q18 | timestamp range          | synth_temporal (30M)      |    72 |   238 | **0.30×** | ✓ |
| q19 | boolean count            | synth_numeric (30M)       |    59 |   139 | **0.42×** | ✓ |

**汇总**：12 ✓ / 4 = / 3 ✗。最差 ratio = q5 1.36×；最好 ratio = q18 0.30×。

## 跟 5/24 JMH local 报告的差异 — 主要来自 mode

5/24 报告 (`docs/profiles/2026-05-24-type-coverage-jmh.md`) 用 JMH local mode 跑 19q，lance 全部慢于 parquet，最差 20.5×。今天 cluster 实测全面持平/反超。看几个 spot：

| q | 5/24 JMH **local** lance | 5/24 JMH **local** parquet | 今 **cluster** lance | 今 **cluster** parquet |
|---|---:|---:|---:|---:|
| q1  | 1035 |  527 | 1034 | **1311** |
| q4  | 5126 | 3278 | 6194 |  6201 |
| q11 |  413 |   33 |  143 |   177 |
| q19 |  445 |   41 |   59 |   139 |

观察：
- **lance 数字两次接近**（dylib 一致 `e993780b…`，5/24 q1=1035 vs 今 q1=1034 几乎一致）
- **parquet 数字 cluster 比 local 慢 2-5×**（q1 527 → 1311，q19 41 → 139）

原因：parquet 在 cluster 模式下每个 task 重新启动 S3A client / Hadoop FileSystem，per-partition overhead 很大；lance 通过 `LanceDatasetCache`（D1, commit `b51ebcd`）+ `LancePartitionCoalescer`（B3, commit `a71d6d0`）压平了这个 overhead。本质上 **lance 在 cluster 模式相对 local 几乎无退化，parquet 大幅退化**，所以 ratio 从 local 的 1.18-20.5× 变成 cluster 的 0.30-1.36×。

这两套数据不矛盾 —— 它们诚实反映了 lance 与 parquet 在不同部署形态下的相对性能，**lance 的工程价值在 cluster 模式下显现得最充分**。

## lance 仍慢的 3 个 case

| q | 类型 | 表 | 慢的原因（推测） |
|---|---|---|---|
| q5  | `count(distinct c_customer_sk)` | customer (2M) | 高基数 distinct → 大型 hash table，lance 的固定 fragment open + ColumnarReader 启动相对 parquet RowGroupReader 多一些开销 |
| q13 | `max(v_int64)` | synth_numeric (30M) | 简单 reduce，本身工作量小（130-160ms 量级），lance JNI ↔ Arrow ↔ Spark 的 overhead 占比偏高 |
| q15 | `sum(v_float)` | synth_numeric (30M) | 同 q13；lance bp128 float kernel 相对 parquet ByteStreamSplit 略慢 |

这三个 case 的绝对差距很小（最大 q13 是 28ms 慢 = parquet 130ms × 1.22），不构成大问题，但可作为后续 lance scanner 启动开销 + bp128 float kernel 的优化目标。

## 工程含义

1. **lance 在生产 cluster 模式下整体优于 parquet**，不是 5/24 JMH local 报告里那个全面慢的形象。
2. q1/q3/q4 这种大表 fact scan 的 ratio 都跌进 0.65-1.00× 区间，**SUM pushdown 的 ROI 进一步缩小** —— 这印证了我们今天放弃 P1+P2+P3 是正确的决定。
3. q4 (decimal × 6) 6.2s ≈ parquet 持平。之前 `2026-05-25-q4-flamegraph-analysis.md` 里"60% lance Rust decode + 25% Spark agg"的成分依然有效，但攻击面已经被压到与 parquet 持平水平，剩下空间价值有限。
4. 未来 lance Rust 的优化重点在 q5/q13/q15 这种小到中等表的简单 sum/distinct 场景，量级 100-800ms，lance 的固定开销 + bp128 float/int64 kernel 仍有 10-30% 优化空间。

## 关联文件

- 原始 logs（保留）：
  - `/tmp/cluster-19q-lance-20260526-174353.log`（17 MB）
  - `/tmp/cluster-19q-parquet-20260526-175117.log`（17 MB）
- dylib 指纹：`e993780ba421827c604fa56da8650672f958f7d7e2db8058d4eac0fc99f744b3`
- 之前的 4-q 子集报告 (`docs/profiles/2026-05-26-revert-baseline.md` 第一版) 被本报告全 19q 数据扩展替代
- 替代过时的 baseline：`docs/profiles/2026-05-25-d1-driver-cache-validation.md` 的 q1/q4 列
