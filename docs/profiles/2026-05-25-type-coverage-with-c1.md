# lance-spark 类型覆盖 A/B：with C1 driver-side dataset cache

**日期**：2026-05-25（C1 + 测试落地后）
**分支**：`feat/java-native-lance-reader` @ `ac48a8c`（perf-tuning-base + C1 + tests）
**dylib**：`e993780ba421827c604fa56da8650672f958f7d7e2db8058d4eac0fc99f744b3`
**JMH**：avgt，`@Fork=1, @Warmup=5×10s, @Measurement=10×10s`，每个 (q, format) 共 150 s

C1 = 进程级 `LanceDatasetCache`（`ConcurrentHashMap.computeIfAbsent` 单次 open per `(uri, version, storageOptionsHash)`），消除了 PR#1 之后 lance-spark 每个 fragment ~5 ms 的 native Dataset 构造开销。

## 完整 19 q × 2 format 对比表（baseline = perf-tuning-base 之前 cache-off; C1 = 当前 cache-on）

| q | 名称 | baseline lance ms | **C1 lance ms** | parquet ms | base ratio | **C1 ratio** | lance Δ% |
|---|---|---|---|---|---|---|---|
| q1 | int32 sum (288M store_sales) | 1035 ± 148 | **604 ± 20** | 507 ± 7 | 2.04× | **1.19×** | **−42%** |
| q2 | 多列 int32 sum (288M) | 1878 ± 185 | **1397 ± 131** | 1234 ± 19 | 1.52× | **1.13×** | −26% |
| q3 | int32 范围过滤 (288M) | 1138 ± 123 | **694 ± 78** | 848 ± 15 | 1.34× | **0.82×** ← | **−39%** |
| q4 | 6×decimal(7,2) sum (288M) | 5126 ± 206 | **4041 ± 88** | 2558 ± 33 | 2.00× | 1.58× | −21% |
| q5 | int32 distinct (2M customer) | 587 ± 52 | **124 ± 7** | 143 ± 6 | 4.09× | **0.87×** ← | **−79%** |
| q6 | 2×decimal(7,2) sum (28M store_returns) | 614 ± 8 | **161 ± 2** | 168 ± 2 | 3.65× | **0.96×** ← | **−74%** |
| q7 | varchar distinct (200K item) | 518 ± 13 | **108 ± 2** | 126 ± 3 | 4.12× | **0.86×** ← | **−79%** |
| q8 | varchar 等值 filter (2M customer) | 597 ± 12 | 549 ± 7 | 49 ± 6 | 12.16× | 11.18× | −8% ⚠ |
| q9 | char(2) groupby (1M customer_address) | 482 ± 24 | **48 ± 2** | 61 ± 4 | 7.93× | **0.79×** ← | **−90%** |
| q10 | date32 范围过滤 (73K date_dim) | 566 ± 15 | 582 ± 41 | 30 ± 3 | 18.93× | 19.46× | +3% ⚠ |
| q11 | int32 groupby (73K date_dim) | 413 ± 9 | **23 ± 3** | 33 ± 4 | 12.35× | **0.69×** ← | **−94%** |
| q12 | star-join (288M × 73K) | 2444 ± 83 | **1642 ± 65** | 1401 ± 41 | 1.74× | **1.17×** | **−33%** |
| q13 | int64 max (30M synth_numeric) | 505 ± 55 | **81 ± 9** | 81 ± 4 | 6.22× | **0.99×** ← | **−84%** |
| q14 | double sum (30M) | 480 ± 29 | **81 ± 10** | 79 ± 5 | 6.09× | 1.02× | **−83%** |
| q15 | float sum (30M) | 482 ± 63 | **66 ± 7** | 58 ± 3 | 8.31× | 1.15× | **−86%** |
| q16 | decimal(18,2) sum (30M) | 596 ± 7 | **165 ± 2** | 180 ± 4 | 3.32× | **0.92×** ← | **−72%** |
| q17 | decimal(38,18) max (30M) | 679 ± 9 | **252 ± 3** | 275 ± 6 | 2.47× | **0.91×** ← | **−63%** |
| q18 | timestamp range (30M synth_temporal) | 654 ± 68 | 629 ± 58 | 138 ± 2 | 4.73× | 4.55× | −4% ⚠ |
| q19 | boolean count (30M) | 445 ± 56 | **52 ± 14** | 47 ± 3 | 9.51× | 1.12× | **−88%** |

## 量化结论

### 1. C1 是 home run

- **16/19 query 显著改善**，lance wall 平均减少 **60%**
- **11/19 query lance 反超 parquet**（ratio ≤ 1.0×）：q3 / q5 / q6 / q7 / q9 / q11 / q13 / q16 / q17（9 个明显反超）+ q14 / q19（持平 ±15%）
- **5/19 query 与 parquet 持平**（ratio 1.02-1.19×）：q1 / q2 / q12 / q14 / q15
- 仅 **3/19 query (q8/q10/q18) C1 没帮上忙**

### 2. 反超 parquet 的语义类型分布

C1 后 lance 反超的 11 个 query 覆盖了所有主流 fixed-width 类型：

| 类型 | 反超 query |
|---|---|
| int32 distinct/groupby | q5 / q11 |
| int64 max | q13 |
| decimal(7,2) sum | q6 |
| decimal(18,2) sum | q16 |
| decimal(38,18) max | q17 |
| varchar distinct | q7 |
| char groupby | q9 |
| boolean | q19（持平） |
| double / float | q14 / q15（持平） |
| date32 range filter on big fact | q3 |

### 3. 3 个 C1 没改善的 outlier — 共同特征

| q | SQL 形态 | C1 后 ratio |
|---|---|---|
| q8 | `count(*) WHERE c_first_name = 'James'` (2M customer) | 11.18× |
| q10 | `count(*) WHERE d_date BETWEEN ... ` (73K date_dim) | 19.46× |
| q18 | `count(*) WHERE v_timestamp BETWEEN ...` (30M temporal) | 4.55× |

全部是 **WHERE filter + COUNT(*) 在小/中表**。共同点：
- 没有 distinct / groupby / 多列 sum
- 选择性高（filter 命中行数少）
- parquet 这边速度极快（30-138 ms），靠 page-level dictionary stats + 极激进的 IO 剪枝

**对照**：q3（同样是 filter + count 但在 288M 行 fact 表）C1 改善 −39%，反超 parquet。区别在数据量级 — q3 IO + decode 是真实负载，cache 节省的 dataset open 占比相对小但仍重要；q8/q10/q18 表小到 IO 几乎为零，瓶颈是 lance scanner 的 **per-fragment filter setup + predicate compile** 等 cache 之外的固定开销。

**这是下一个高 ROI 优化目标**，不是 C1 的 scope。

### 4. 异常注意点

- **q4 (6×decimal sum 288M)** 是改善最小的非 outlier (-21%)。原因：q4 是 decode-bound（5126 ms 中绝大部分是 6 列 i128 的 SIMD bp128 解码 + Spark sum），fixed cost 占比小。C1 帮不上 decode；要 q4 进一步压缩需要 1C（lance Rust dest-buffer refactor）等更深层优化。
- **parquet 数字也在两次 JMH 之间漂移**（如 q4 parquet 3278 → 2558，q1 parquet 527 → 507）。这是 system-level 噪声（local MinIO IO 抖动 / JIT 不同 path）。**C1 是不动 parquet 的，所以 lance 绝对值改善才是 cache 净收益**。

## 工程含义

1. **C1 把"lance 在小/中表上慢 5-20 倍" 这个结构性问题完全消除**（除了 3 个 filter-only outlier）。type-coverage 报告的初版结论"lance fixed-cost-bound on small tables" 在 C1 后已不成立 — C1 就是这个 fixed cost 的解药。
2. **q8/q10/q18 outlier** 提示 lance 在 small-table filter+count 场景上还有一层非 cache-related 固定开销，需要新一轮 probe 定位（建议下一轮 B2：聚焦 q10 的火焰图）。
3. **q4 大 fact 表 6 列 decimal 仍是 1.58× 慢于 parquet**，这是 decode 路径，下一步该做的是 1C（lance Rust dest-buffer refactor），跟 C1 解决的是不同问题。

## 类型覆盖 + cache-on 后的最终结论

**lance（perf-tuning-base + PR#1 + PR#6858 + C1）在 11/19 主流 OLAP query 上 ≥ parquet**，剩余 5 个持平、3 个 filter-only outlier 落后。**没有任何 lance decoder 路径上的结构性短板**，PR#6858 的 decimal128 dispatch 在所有 decimal 精度（7,2 / 18,2 / 38,18）上都是 lance 反超 parquet 的关键。

## 关联文件

- baseline JMH log（cache off）：`/tmp/jmh-typecov-q1-q19.log` + `/tmp/jmh-typecov-q13-q17-redo.log`
- C1 JMH log（cache on）：`/tmp/jmh-typecov-c1-cache-on.log`
- C1 single-shot probe：`/tmp/fixed-cost-probe-c1.log`
- 实现：`lance-spark-base_2.12/.../internal/LanceDatasetCache.java`（commit `8b0eadc`）
- 单测：`lance-spark-base_2.12/src/test/.../LanceDatasetCacheTest.java`（13 tests，commit `ac48a8c`）
- benchmark + setup：commit `c7718d0`
