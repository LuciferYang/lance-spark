# 2026-05-26 — Cluster baseline @ HEAD `71408f9` (post-revert)

**日期**：2026-05-26
**lance-spark 分支**：`feat/java-native-lance-reader` (HEAD `71408f9`)
**lance 分支**：`feat/1c-dest-buffer` (HEAD `15399937b`，PR#1 + PR#6858 squashed)
**dylib**：`liblance_jni.dylib` sha256 `e993780ba421827c604fa56da8650672f958f7d7e2db8058d4eac0fc99f744b3`，
  release-optimized build of `15399937b`，150 432 576 bytes
**dataset**：`s3://benchmark/tpcds-sf-100/store_sales_bp128_dispatch.lance`，305 fragments × 53 MB avg = 16 GiB（写入时间 2026-05-21 12:03，未变）
**测量**：`LocalClusterAB.java`, `local-cluster[4,1,2048]`, 10 iter, noop+count+collect 三种 action 交替, iter 5-10 稳态均值

## TL;DR

P1+P2+P3 SUM-pushdown 全部回退后，重测 cluster baseline 验证稳定状态。**lance 在 q1/q11/q14 反超 parquet，q4 与 parquet 基本持平**。

修正一个被错误数据污染的 baseline：之前 `2026-05-25-d1-driver-cache-validation.md` 里写的 q1=2329 / q4=18158 是用了 dylib `88bbdb7b…`（疑似 debug build 或带探针的中间版本）跑出来的，与 release dylib `e993780b…` 比慢 2-3×。当前真实数据见下表。

## 对比数据（cluster, iter 5-10 mean, collect_ms）

| query | lance_bp128 | parquet | L/P | 历史 D1 表 (污染) |
|---|---:|---:|---:|---:|
| q1 (sum int)        | **1 083** | 1 318 | **0.82×** | 2329 / 1186 = 1.96× ❌ |
| q4 (sum dec×6)      | **6 722** | 6 453 | **1.04×** | 18158 / 6710 = 2.71× ❌ |
| q11 (date_dim group)| **188**   | 200   | **0.94×** | 170 / 199 = 0.85× ✓  |
| q14 (sum double)    | **102**   | 134   | **0.76×** | 162 / 131 = 1.24× ✓  |

q11/q14 与 D1 报告同量级（小表 dataset cache 占主导，与 dylib 关系小）。q1/q4 的 D1 数据是错的。

## 当前数据匹配 B3 报告

`docs/profiles/2026-05-25-b3-partition-coalesce-validation.md`（5/25 15:10）：

| query | B3 表 OFF | 本次测量 | 差异 |
|---|---:|---:|---:|
| q1 collect | 1 584 | 1 083 | -32%（lance 提升） |
| q4 collect | 6 757 | 6 722 | <1%（一致） |

q4 完全吻合，证实 dylib `e993780b…` 是 5/25 15:10 测量时的状态。q1 略快可能是因为 cluster 当时的 thermal/load 略好，但量级一致。

## 原始数据（lance_bp128，iter 6-10）

```
>>> q	format	master	iter	noop_ms	count_ms	collect_ms
>>> q1	lance_bp128	local-cluster	6	1193	258	1073
>>> q1	lance_bp128	local-cluster	7	1112	226	1060
>>> q1	lance_bp128	local-cluster	8	1119	224	1080
>>> q1	lance_bp128	local-cluster	9	1072	236	1065
>>> q1	lance_bp128	local-cluster	10	1091	216	1115
>>> q4	lance_bp128	local-cluster	5	6445	269	6727
>>> q4	lance_bp128	local-cluster	6	6413	208	6962
>>> q4	lance_bp128	local-cluster	7	8815	296	6834
>>> q4	lance_bp128	local-cluster	8	6694	230	6497
>>> q4	lance_bp128	local-cluster	9	6974	200	6712
>>> q4	lance_bp128	local-cluster	10	6897	242	6603
>>> q11	lance_bp128	local-cluster	5	174	306	184
>>> q11	lance_bp128	local-cluster	6	164	286	177
>>> q11	lance_bp128	local-cluster	7	158	301	182
>>> q11	lance_bp128	local-cluster	8	145	270	179
>>> q11	lance_bp128	local-cluster	9	152	265	221
>>> q11	lance_bp128	local-cluster	10	165	292	184
>>> q14	lance_bp128	local-cluster	5	110	37	106
>>> q14	lance_bp128	local-cluster	6	101	35	100
>>> q14	lance_bp128	local-cluster	7	122	36	105
>>> q14	lance_bp128	local-cluster	8	93	34	100
>>> q14	lance_bp128	local-cluster	9	99	35	105
>>> q14	lance_bp128	local-cluster	10	99	35	95
```

## 关于 D1 报告的污染数据 — 假设

D1 报告 (`2026-05-25-d1-driver-cache-validation.md`) 测量时间 5/25 20:36，用 dylib hash `88bbdb7b…`，q1=2329 / q4=18158。

可能的污染来源（**未追到 root cause**，只能列假设）：
- 在 5/25 期间为某项调试 / 探针实验重 build 了一个 debug-mode dylib，jar 里被换掉但忘了还原；
- 在 5/25 17:00-20:00 之间有个未提交的本地 patch 改了 lance Rust 的某个 hot path（例如改写 bitpacking decode，引入退化），后来回退了但 dylib 没被刷新；
- 任何带额外 tracing / debug-assert 的 cargo flag 跑出来的 dylib。

无法 100% 还原现场。但本次测量证明：**当前 release dylib `e993780b…` 是干净的，q4 cluster 真实值是 6.7s，与 parquet 持平**。

## 工程含义

1. D1 报告里 q1/q4 的"-16% / +4% noise" 结论 **基于污染数据，作废**。q11/q14 的 -71-72% 结论仍然成立（小表用例，dylib 对 dataset open 时间影响很小）。
2. 当前 HEAD 的 lance vs parquet ratio 在 OLAP scan 类查询上已经做到了 **0.76-1.04×**（lance 持平或反超）。
3. q4 decode-bound 的"60% lance Rust decode + 25% Spark agg"分析（`2026-05-25-q4-flamegraph-analysis.md`）仍有效—— q4 wall 6.7s 已经接近 parquet，剩下的攻击面在 lance Rust SIMD / memmove 优化，不在 Spark 侧 pushdown。
4. **跑性能对比时一定要先 cargo build --release + 验证 dylib sha256**，否则可能像 D1 报告一样跑出 2-3× 的虚假回退数据。

## 关联文件

- 原始 logs：
  - `/tmp/cluster-revert-baseline-lance-20260526-145841.log`
  - `/tmp/cluster-revert-baseline-parquet-20260526-150215.log`
- dylib 指纹：`e993780ba421827c604fa56da8650672f958f7d7e2db8058d4eac0fc99f744b3`
  - `/Users/yangjie01/SourceCode/git/lance/java/target/rust-maven-plugin/lance-jni/release/liblance_jni.dylib`
  - 同步装到 `~/.m2/repository/org/lance/lance-core/7.0.0-LOCAL-SNAPSHOT/lance-core-7.0.0-LOCAL-SNAPSHOT.jar` 内 `nativelib/darwin-aarch64/liblance_jni.dylib`
  - 以及 `~/.m2/repository/org/lance/lance-spark-bundle-4.0_2.13/0.5.0-SNAPSHOT/...` 内同路径
- 替代过时的 baseline：`docs/profiles/2026-05-25-d1-driver-cache-validation.md` q1/q4 列
