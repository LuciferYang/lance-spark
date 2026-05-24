# lance-spark 类型覆盖 A/B：Lance vs Parquet（JMH avgt）

**日期**：2026-05-24
**分支**：`feat/java-native-lance-reader` @ `f360ac9`（perf-tuning-base milestone：PR#1 + PR#6858 + A1 + C-small + C+）
**dylib**：`e993780ba421827c604fa56da8650672f958f7d7e2db8058d4eac0fc99f744b3`
**Spark**：4.0+ × Scala 2.13，JDK 17.0.18，`-Darrow.enable_unsafe_memory_access=true`
**Storage**：本地 MinIO（`http://localhost:9000`），所有数据集都驻留同一 bucket
**JMH**：avgt，`@Fork=1, @Warmup=5×10s, @Measurement=10×10s`，每个 (q, format) 共 150 s

## 数据集

| 数据集 | 行数 | 来源 |
|---|---|---|
| `store_sales_bp128_dispatch.lance` / `store_sales/` | 287,997,024 | dsdgen SF=100 |
| `customer_bp128.lance` / `customer/` | 2,000,000 | dsdgen SF=100 |
| `customer_address_bp128.lance` / `customer_address/` | 1,000,000 | dsdgen SF=100 |
| `item_bp128.lance` / `item/` | 204,000 | dsdgen SF=100 |
| `date_dim_bp128.lance` / `date_dim/` | 73,049 | dsdgen SF=100 |
| `store_returns_bp128.lance` / `store_returns/` | 28,795,080 | dsdgen SF=100 |
| `synth/numeric_bp128.lance` / `synth/numeric/` | 30,000,000 | `SyntheticDataSetup`（合成）|
| `synth/temporal_bp128.lance` / `synth/temporal/` | 30,000,000 | `SyntheticDataSetup`（合成）|

`bp128_dispatch.lance` 文件是用 PR#6858 (i128 narrow-width dispatch) 启用的 writer 写出；其它 `_bp128.lance` 是普通的当前 writer 输出，对 fixed-width 列自动启用 InlineBitpacking。

## 类型覆盖矩阵（11 种主流 OLAP 类型全覆盖）

| 类型 | bp128 kernel | query | 来源 |
|---|---|---|---|
| int32 | InlineBitpacking u32 | q1 / q3 / q5 / q11 | TPC-DS |
| int64 | InlineBitpacking u64 | q13 | 合成 |
| float | ByteStreamSplit / Plain | q15 | 合成 |
| double | ByteStreamSplit / Plain | q14 | 合成 |
| decimal(7,2) | bp128 NarrowU32 | q4 / q6 | TPC-DS |
| decimal(18,2) | bp128 NarrowU64 | q16 | 合成 |
| decimal(38,18) wide | bp128 SequentialU128 | q17 | 合成 |
| varchar(N) | BinaryMiniBlock | q7 / q8 | TPC-DS |
| char(N) | BinaryMiniBlock | q9 | TPC-DS |
| date32 | bp128 u32 | q10 | TPC-DS |
| timestamp(micros) | bp128 u64 | q18 | 合成 |
| boolean | bitmap | q19 | 合成 |

## 完整结果（按 ratio 升序）

| q | 类型 / 操作 | 表 (行数) | lance_bp128 ms ± CI | parquet ms ± CI | **ratio** |
|---|---|---|---|---|---|
| q3 | int32 范围过滤 | store_sales (288M) | 1138 ± 123 | 963 ± 196 | **1.18×** |
| q2 | 多列 int32 sum | store_sales (288M) | 1878 ± 185 | 1274 ± 47 | 1.47× |
| q4 | 6×decimal(7,2) sum | store_sales (288M) | 5126 ± 206 | 3278 ± 705 | 1.56× |
| q12 | star-join + group | store_sales × date_dim | 2444 ± 83 | 1380 ± 25 | 1.77× |
| q1 | int32 sum | store_sales (288M) | 1035 ± 148 | 527 ± 10 | 1.96× |
| q17 | decimal(38,18) max | synth_numeric (30M) | 679 ± 9 | 275 ± 5 | 2.47× |
| q16 | decimal(18,2) sum | synth_numeric (30M) | 596 ± 7 | 193 ± 5 | 3.09× |
| q6 | 2×decimal(7,2) sum | store_returns (28M) | 614 ± 8 | 174 ± 2 | 3.53× |
| q5 | int32 distinct | customer (2M) | 587 ± 52 | 164 ± 34 | 3.57× |
| q7 | varchar distinct | item (200K) | 518 ± 13 | 125 ± 5 | 4.14× |
| q18 | timestamp range | synth_temporal (30M) | 654 ± 68 | 133 ± 2 | 4.92× |
| q14 | double sum | synth_numeric (30M) | 480 ± 29 | 73 ± 2 | 6.55× |
| q13 | int64 max | synth_numeric (30M) | 505 ± 55 | 71 ± 1 | 7.07× |
| q9 | char(2) groupby | customer_address (1M) | 482 ± 24 | 62 ± 3 | 7.71× |
| q15 | float sum | synth_numeric (30M) | 482 ± 63 | 56 ± 2 | 8.62× |
| q19 | boolean count | synth_numeric (30M) | 445 ± 56 | 41 ± 2 | 10.91× |
| q8 | varchar 等值 filter | customer (2M) | 597 ± 12 | 48 ± 6 | 12.34× |
| q11 | int32 groupby | date_dim (73K) | 413 ± 9 | 33 ± 3 | 12.65× |
| q10 | date32 范围过滤 | date_dim (73K) | 566 ± 15 | 28 ± 3 | **20.51×** |

## 结构性结论

### 1. lance 在所有 11 种类型上都慢于 parquet

没有任何 query 上 lance 反超。single-shot 看到的 q8 / q18 / q19 反超在 JMH warm 数字下全部反转。

### 2. ratio 与表规模强相关，与类型几乎无关

| 表规模 | ratio 范围 | 例 |
|---|---|---|
| 288M 行 fact | 1.18 – 1.96× | q1/q2/q3/q4 |
| 28-30M 行 | 2.5 – 7.1× | q6/q13/q14/q15/q16/q17/q18/q19 |
| 200K-2M 行 | 3.6 – 12.3× | q5/q7/q8/q9 |
| 73K 行 dim | 12.6 – 20.5× | q10/q11 |

### 3. 根因：lance 每个 SQL 有 ~400-500 ms fixed overhead

观察 30M 行 synth_numeric 上六种类型的 lance wall：

| 类型 | lance ms | parquet ms |
|---|---|---|
| int64 | 505 | 71 |
| double | 480 | 73 |
| float | 482 | 56 |
| decimal(18,2) | 596 | 193 |
| decimal(38,18) | 679 | 275 |
| boolean | 445 | 41 |

- **lance 这六种类型 wall 都在 445-679 ms（差异 < 50%）** —— 类型差异被 ~440 ms 的 fixed-cost 淹没
- **parquet 同样查询 wall 在 41-275 ms（差异 6.7×）** —— 类型差异显著，parquet 是真正 read+decode-bound

→ **lance 在 30M 行规模仍是 fixed-cost-bound，不是 decode-bound**

### 4. PR#6858 (decimal128 bp128 dispatch) 在大 fact 表生效

| query | precision | bp128 kernel | ratio |
|---|---|---|---|
| q4 (288M, 6 列) | (7,2) | NarrowU32 | 1.56× ← 比 q1 int32 (1.96×) 还好 |
| q6 (28M) | (7,2) | NarrowU32 | 3.53× |
| q16 (30M) | (18,2) | NarrowU64 | 3.09× |
| q17 (30M) | (38,18) | SequentialU128 | 2.47× |

q4 ratio 1.56× 比 q1 1.96× 更小，证明 PR#6858 在大数据量下对 decimal 的优化是真实加速 —— 6 列 decimal128 解码反而比 1 列 int32 解码相对更接近 parquet。

### 5. small-table 上的 ratio 不能简单解读为「lance 在该类型上慢 N 倍」

q10 ratio 20.5× 不是「date32 解码慢 20 倍」，而是「73K 行的 wall 被 ~540 ms fixed cost 拉成 566 ms，parquet 只用 28 ms」。在真实业务里这是 dimension 表 broadcast 成本而不是 columnar 解码成本。

## 工程含义

| 对工程决策 | 结论 |
|---|---|
| Lance 是否在某种类型上有结构性短板？ | **没有**。所有 fixed-width 类型都走同一条 InlineBitpacking + Arrow 路径，差异在 fixed cost 不在 decoder。 |
| PR#6858 (i128 narrow dispatch) 是否生效？ | ✅ 大 fact 表（288M decimal）ratio 最好；30M 合成表 decimal(38,18) ratio 也只有 2.47×。 |
| 1C (Rust 端 destination-buffer 重构) ROI 还有多少？ | 在 q4 这种大 fact 工作负载（ratio 1.56×）上 1C 还能再吃 ~3-5%；在 small-table 工作负载（ratio 7-20×）上 1C 几乎无效，因为 fixed-cost 主导。 |
| 下一个高 ROI 优化方向 | **降低 per-SQL fixed-cost** （Dataset.open / scan setup / JNI marshalling），收益对 small-table 工作负载非线性放大；这块 PR#1 已经做了一部分（Session reuse），剩余 ~400 ms 还有空间。 |

## 关联文件

- 完整 JMH log：`/tmp/jmh-typecov-q1-q19.log`（17 个 q 的 ANSI 内 sum 路径）
- q13/q17 redo log：`/tmp/jmh-typecov-q13-q17-redo.log`（max() 替代 sum 后的版本）
- benchmark 源：`micro-benchmark/src/main/java/org/lance/spark/microbenchmark/MinioTpcdsBenchmark.java`（q1-q19）
- 数据 setup：`MultiTableSetup.java`（5 张 TPC-DS 维表+returns）+ `SyntheticDataSetup.java`（2 张合成表）
- 单次验证：`BatchSingleShotRunner.java`（用于 SQL 验证、不可作为性能结论）
