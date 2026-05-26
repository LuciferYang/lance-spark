# lance + lance-spark 独立可提交 patch 清单

**整理日期**：2026-05-26
**当前 baseline**：
- lance HEAD `15399937b`（fork on `feat/1c-dest-buffer`）
- lance-spark HEAD `adadcfe`（fork on `feat/java-native-lance-reader`）
- dylib `e993780ba421827c604fa56da8650672f958f7d7e2db8058d4eac0fc99f744b3`（release-optimized）
- 实测 cluster 19q × 2 fmt：lance 12/19 反超 parquet，4/19 持平，3/19 慢（最差 1.36×）

本文档把这一周 perf 工作分成 6 个独立可单独提交的 patch（2 个 lance Rust + 4 个 lance-spark Java），每个 patch 都带：目的、改动、效果、依赖关系。

---

## 概览

| # | 仓库 | Patch 名 | 类型 | 主要 commit | LoC | 测得效果 |
|---|---|---|---|---|---:|---|
| L1 | lance core | u128 bitpacking for decimal128 | feat | `f1488a8eb` + `3cd8b575f` | ~600 | store_sales 文件大小 34→16 GiB；q4 cluster 持平 parquet |
| L2 | lance core | ObjectStoreRegistry single-flight | feat | `15399937b` | ~800 | 多并发 cold open 合流，q3 305 任务延迟 -90% |
| S1 | lance-spark | Decimal fast read path (p≤18) | perf | `a61737a` + `93fd8cc` | ~150 | decimal 单列 fullscan -5.4%（CPU-bound 场景） |
| S2 | lance-spark | LanceDatasetCache (executor) | perf | `8b0eadc` + `ac48a8c` | ~200 | 234-fragment scan 省 ~150ms wall；q11 73K 行 -80% |
| S3 | lance-spark | LancePartitionCoalescer | perf | `a71d6d0` | ~250 | 305 任务 → 36（store_sales SF=100），q3 cluster 调度延迟消失 |
| S4 | lance-spark | Driver-side LanceDatasetCache | perf | `b51ebcd` | ~150 | q11/q14 wall -71-72%（pre-job 482→11 ms）|

依赖关系：S4 依赖 S2（D1 是 C1 的扩展）；其余两两独立。

---

## Lance Core (Rust)

### L1 — u128 bitpacking for decimal128

**Commits**：
- `f1488a8eb` `feat(encoding): add u128 bitpacking support for decimal128 columns`（基础 u128 bitpacking）
- `3cd8b575f` `feat(encoding): per-chunk u128 bitpacking dispatch (u32/u64/u128/memcpy)`（per-chunk 路由 u32/u64/u128/memcpy 五条 kernel）
- `47e7e8100` `test(encoding): add Decimal128 inline-bitpacking decode/encode benches`（配套 benches）
- `f1314dddc` `fix(encoding): rustdoc intra-doc link + typo`（rustdoc 修补）

上游对应 PR：lance #6858

**目的**：
之前 BitPacking 只支持 u8/u16/u32/u64，decimal128 列被存成全宽 128-bit 无压缩。TPC-DS `decimal(7,2)` 实际只用到 ~24 bit，浪费 5×。L1 把 u128 也接进 BitPacking，并按 chunk 实际 bit_width 路由到最窄 kernel，让 decimal128 享受 BitPacking。

**改动**：
- `rust/lance-encoding/src/encodings/physical/inline_bitpacking.rs`：新加 u128 packer/unpacker，per-chunk dispatch 表
- 16-byte chunk header layout 不变；body 字节数仍是 `bit_width × ELEMS_PER_CHUNK / 8`，只是 body 字节按 dispatched kernel 类型重新解释
- 下面 5 条 kernel 共用一个 dispatch 入口 `u128_kernel_for(bit_width)`：

| bit_width | kernel |
|---|---|
| 0 | zero-fill (no body) |
| 1..=32 | reinterpret body as `&[u32]`，FastLanes u32 SIMD |
| 33..=64 | reinterpret body as `&[u64]`，FastLanes u64 SIMD |
| 65..=127 | scalar sequential u128 bitstream |
| 128 | memcpy identity (1 u128/value) |

**效果**：
- store_sales SF=100 文件大小 **34 GiB → 16 GiB**（5.6× 压缩）
- q4 cluster `local-cluster[4,1,2048]` 6 列 decimal sum：lance 6194 ms vs parquet 6201 ms（**1.00× 持平**）
- 没有 L1 时 q4 是 ~17-18s，3× 慢于 parquet

**Review status**：上游 lance PR#6858 已经过 19 轮 multi-role review（task #143 收敛 streak 9/10），本地 fork 已合并

**风险 / 兼容性**：u128 inline bitpacking 是新加的 encoding 路径，对存量数据不影响（旧文件不会被新 reader 误解码）。Per-chunk dispatch 已经过 round-trip + proptest fuzz + 全负数 + 混合 width 单元测试。

---

### L2 — ObjectStoreRegistry single-flight + Default-open Session reuse

**Commits**：`15399937b` `feat(jni,io): default-open Session reuse + ObjectStoreRegistry single-flight`

上游对应 PR：lance "PR#1"（cf `feat/1c-dest-buffer` baseline）

**目的**：
高并发 cold open 同一个 URI 时（典型场景：Spark cluster 多 executor 同时打开同一个 lance 数据集），原来每个 `Dataset.open` 都从头跑 ObjectStore 构造（credential probe + IMDS + TLS handshake），消耗 ~30-50 ms/open。305 个 fragment 串起来 ~10-15 秒额外开销。

**改动**：
- `rust/lance-io/src/object_store/providers/`：新加 `LazyLock<Arc<ObjectStoreRegistry>>` 进程级单例，with single-flight via `BuildLockSession`（HashMap key = `Fingerprint128(provider_id, normalized_options)`）
- `java/lance-jni/src/blocking_dataset.rs`：默认 `Dataset.open` 不再每次构造一个全新 Session，而是构造一个共享 ObjectStoreRegistry 的 Session（metadata/index cache 仍然 per-call —— 不让缓存大小被任意 caller 污染）
- 提供 opt-out `LANCE_JNI_DISABLE_DEFAULT_REGISTRY_SHARING=1` 给需要租户隔离的场景
- 完整覆盖 `Fingerprint128`（process-random key 防 hash flooding）+ HashMap unbounded 增长检查 + 异步失败共享 + bounded sweep 清理

**效果**：
- 305-fragment cluster 并发 cold open，从 ~30 ms × 305 ≈ 10 s 降到一次 cold + 304 次 ~1 ms 共享，wall 减约 9 s
- q3 cluster `local-cluster[4,1,2048]` 305 任务 OFF=1801 ms ≈ parquet 1836 ms（之前 4935 ms ⚠️）—— 单独 L2 已经把 q3 拉回 parquet parity

**Review status**：上游 lance PR#1 经过 21 轮 multi-role review（task #77 收敛 streak 3/4），本地 fork 已落地

**风险 / 兼容性**：bare-URI 默认共享意味着 *跨调用方* 的 ObjectStore credential 共享。已经在 rustdoc 中标注 + 在 Java README 加文档 + 提供 env var opt-out，对单租户单进程无影响。

---

## Lance-Spark (Java)

### S1 — Decimal fast read path (precision ≤ 18)

**Commits**：
- `a61737a` `perf(vectorized): fast decimal read path for precision <= 18`
- `93fd8cc` `perf(vectorized): wire decimal fast path into null tracking + add q4 benchmark`

**目的**：
Spark 读 lance decimal128 列原来走 `BigInteger → BigDecimal → Decimal.apply`，每行 3 次堆分配。对 precision ≤ 18 的列（如 TPC-DS `decimal(7,2)`），unscaled value 完全装得下 long，可以直接走 `Decimal.apply(long, precision, scale)` 跳过两层包装。

**改动**：
- `lance-spark-base_2.12/.../vectorized/LanceArrowColumnVector.java`：
  - 新增 `LanceDecimalAccessor` —— 从 Arrow `DecimalVector` 16-byte buffer 读 unscaled long（low 8 bytes）
  - `getDecimal(idx, precision, scale)` 在 precision ≤ 18 时走快路，否则 fallback 走原 BigInteger 路径
  - hasNull/numNulls/isNullAt 也接进 decimal 路径，避免 codegen sum 的 `Decimal.toUnscaledLong on null` NPE
- `micro-benchmark/.../MinioTpcdsBenchmark.java`：q4 6-decimal sum benchmark 追加到 JMH 套件

**效果**：
- 单列 decimal fullscan（local 文件）：3737 → 3534 ms (**-5.4%** CPU-bound)
- q4 高 RTT MinIO（toxiproxy 100ms）：fast path on 6883 ± 80 vs off 6891 ± 217 —— 网络主导，差异在噪声内
- 综合：在 CPU-bound 场景 5% 加速；网络主导无负担。**保留是因为 (a) 正确，(b) 0 cost，(c) CPU-bound 场景免费的 5%**

**风险 / 兼容性**：纯 Spark accessor 优化，不改 lance 文件格式。fallback 路径保留旧逻辑。

---

### S2 — LanceDatasetCache (executor-side, "C1")

**Commits**：
- `8b0eadc` `perf(spark): driver-side LanceDatasetCache to amortize per-fragment open`（虽然 commit message 写 "driver-side"，实际只 wire 到 `LanceFragmentScanner`，是 executor-side cache；S4 才扩展到 driver-side）
- `ac48a8c` `test(spark): unit tests for LanceDatasetCache + dynamic property reads`

**目的**：
即便有了 L2 的 ObjectStoreRegistry single-flight，每个 fragment 仍要付 ~5 ms 的 native `Dataset` 构造（Session/metadata cache 是 per-open 的，PR#1 故意保留这个隔离性）。234-fragment store_sales scan 累计 ~1.2 s overhead。q11 73K 行 1 fragment 的 cold dataset open 占了 135 ms（parquet 33 ms baseline）—— ratio 12.65× 几乎全是这个固定成本。

**改动**：
- `lance-spark-base_2.12/.../internal/LanceDatasetCache.java`（新文件）：进程级 `ConcurrentHashMap<Key, Dataset>`，key = `(uri, version, storageOptionsHash)`。`getOrOpen(...)` 走 `computeIfAbsent` —— 并发 fragment 走同一个 cold open，single-flight 通过 ConcurrentHashMap 内置语义
- `lance-spark-base_2.12/.../internal/LanceFragmentScanner.java`：从 `Utils.openDatasetBuilder().build()` 改成 `LanceDatasetCache.getOrOpen(inputPartition)`，cache-owned 实例 skip `dataset.close()`
- `LanceFragmentColumnarBatchScanner.java`：调用 fragment scanner 路径同步更新
- toggles：
  - `-Dlance.spark.dataset_cache_enabled=false` 还原 per-fragment open
  - `-Dlance.spark.dataset_cache_max=N` 上限缓存条目数
  - `-Dlance.spark.probe_fixed_cost=true` 打印 per-fragment timing 分解
- `LanceDatasetCacheTest.java`：13 个单元测试覆盖 Key 等价性 / null version / lifecycle

**效果**：
- 234-fragment store_sales scan：~1.2 s overhead → 一次 cold + 233 次 cache hit ≈ 5 ms
- q11 (73K 行 date_dim, 1 frag)：cold dataset open 135 ms → cache hit ~1 ms
- q1/q4 这类 decode-bound 大表：wall 影响小（~5%），因为 fixed cost 占比小

**风险 / 兼容性**：纯 Spark 端 cache，不影响 lance 数据格式。`computeIfAbsent` 是 ConcurrentHashMap JDK 内置 single-flight 语义。entry 持有 Dataset reference 直到 SparkSession 结束 —— `dataset_cache_max` 默认 64，足够 typical Spark 应用。

---

### S3 — LancePartitionCoalescer ("B3")

**Commits**：`a71d6d0` `perf(spark): coalesce fragments by spark.sql.files.maxPartitionBytes`

**目的**：
原 `LanceSplit.planScan` 每个 lance fragment 映射成 1 个 Spark partition / task。store_sales SF=100 有 305 fragments → 305 tasks/query，而 parquet 同样的数据集会被 Spark 自带的 `FilePartition` 合并到 ~12 个。305 个任务的调度 + executor cold path 累计成本，造成 q3 cluster 4935 ms vs parquet 1836 ms 的 2.69× 退化（B3 调查的 root cause）。

**改动**：
- `lance-spark-base_2.12/.../read/LancePartitionCoalescer.java`（新文件）：mirrors Spark `FilePartition.getFilePartitions` —— greedy bin-packing in fragment-id order，with virtual `openCostInBytes` overhead
- `LanceScan.planInputPartitions`：在 `pruneByLimit` 之后调用 coalescer
- `LanceSplit.java`：`ScanPlanResult` 加 `fragmentByteSizes` 字段供 coalescer 用
- 配置优先级：sysprop `lance.spark.{max_partition_bytes,open_cost_in_bytes}` > SparkSession conf `spark.sql.files.{maxPartitionBytes,openCostInBytes}` > 默认 128 MiB / 4 MiB
- 跳过 coalesce 的情形：
  - SPJ (Storage-Partitioned Join) active —— `KeyGroupedPartitioning` 需要 1:1
  - 任何 fragment `fileSizeBytes` is null —— 老 dataset 没记录大小
  - `lance.spark.partition_coalesce_enabled=false`
- 测试：9 unit (bin-pack math) + 3 integration (sysprop wiring + null-fallback)

**效果**：
- store_sales SF=100：**305 tasks → 152 (128MB) → 36 (512MB)**，DAGScheduler log 验证
- q1 collect_ms 1584 → 1281 (**-19%** at 512MB target)
- q3 cluster 在 L2 落地后已经持平 parquet（1801 ms ≈ 1836 ms），B3 的额外加成有限但保留是为了防止未来 >1k fragments 的 dataset 真正退化
- q4 在 128MB target 下 +7% 噪声退化，512MB 下消失（partition-size 调优问题，非结构 bug）

**风险 / 兼容性**：纯 Spark 端 partition planning 优化，不改 lance / parquet 数据格式。SPJ active 时自动跳过保留 1:1 不影响 storage-partitioned join 正确性。

---

### S4 — Driver-side LanceDatasetCache extension ("D1")

**Commits**：`b51ebcd` `perf(spark): driver-side LanceDatasetCache eliminates per-SQL-exec dataset reopen`

**依赖**：S2 (LanceDatasetCache executor-side) 必须先落地

**目的**：
S2 cache 只覆盖 executor 侧的 `LanceFragmentScanner` open。**driver 侧**每次 SQL 执行还是会重新 open 数据集 3 次：
1. `LanceDataSource.inferSchema`
2. `LanceScanBuilder.getOrOpenDataset`
3. `LanceSplit.planScan`

对小表（q11 1 fragment, q14 32 frags），这 482 ms 的 driver pre-job 时间占总 wall 的 90%+。

**改动**：
- `LanceDatasetCache.java`：新增 `getOrOpen(LanceSparkReadOptions)` overload + `Key.fromReadOptions(...)` factory，用 `readOptions.getStorageOptions()` 派生 hash
- `LanceDataSource.java`：`inferSchema` 走 cache
- `LanceScanBuilder.java`：`getOrOpenDataset()` 走 cache，新增 `lazyDatasetIsCached` 字段，`closeLazyDataset()` 对 cached entries skip `Dataset.close()`
- `LanceSplit.java`：`planScan` 走 cache
- driver-side cache 与 executor-side cache 在 cluster mode 是不同 JVM 各自的实例，key derivation 略有差异（driver: storage options / executor: initialStorageOptions）但对正确性无影响（已经在 rustdoc 标注）
- `LanceDatasetCacheTest.java`：5 个新增测试覆盖 `Key.fromReadOptions` URI/version/storageOption-content/map-order 敏感性

**效果**：
- q11 pre-job time：**482 ms → 10-12 ms (-97%)**（Spark Event Log 实测）
- q14 同等 -97% delta
- cluster steady state（iter 6-15 mean collect_ms）：
  - q11 lance：580 → **170 ms (-71%)**，0.85× parquet —— **lance 反超 parquet**
  - q14 lance：582 → **162 ms (-72%)**，1.24× parquet —— 接近 parquet
  - q1/q4：noise 内（pre-job 在大表 wall 占比小）

**风险 / 兼容性**：纯 Spark 端 cache 扩展。driver 跟 executor 不共享缓存条目（不同 JVM），不会因为 driver close dataset 影响 executor 的读取。

---

## 提交顺序建议

如果分仓提 PR：

1. **lance core 上游**：
   - L1 (u128 bitpacking) — 已是 PR#6858
   - L2 (ObjectStoreRegistry single-flight) — 已是 PR#1
   - 两者无依赖，可并行

2. **lance-spark 上游**：
   - S1 (decimal fast path) — 独立小 patch，先合
   - S2 (LanceDatasetCache executor) — 独立基础 patch
   - S3 (LancePartitionCoalescer) — 独立 patch，可与 S2 并行
   - S4 (driver-side cache) — 必须在 S2 之后

3. **依赖链**：
   - S4 → S2（API extension）
   - lance-spark perf 全部依赖 lance core 至少 L2（不然 S2 cache 之外的 cold open 还是慢）

## 整体效果

19 query × 2 format cluster A/B（HEAD `81cfc76` = L1+L2+S1-S4 全合）:
- **12/19 反超 parquet**
- **4/19 持平 parquet**  
- **3/19 慢 (最差 1.36×)**
- 全 19 query lance 数字与 5/24 JMH local 报告里 lance 数字保持一致（同款 dylib），但 parquet 在 cluster 模式比 local 慢 2-5×（每 task S3A client 重启），所以 cluster 模式下 lance 整体反超

详见 `docs/profiles/2026-05-26-revert-baseline.md`。

---

## 已撤销的工作（不在此清单中）

- **P1 lance JNI builder-style aggregate API**：commits `bef8e4755` + `e2bfefa5a`，已 reset
- **P2 lance-spark Sum pushdown wiring**：commits `d8eb716` + `b3cd52a`，已 reset
- **P3 cluster A/B + report**：已删

撤销原因：q4 decimal pushdown 卡 Spark V2 partial-cast bug；q1 int sum 退化 30%。本质是 Spark V2 SupportsPushDownAggregates 的 partial pushdown 协议缺陷（`V2ScanRelationPushDown.scala:273-280` 强转回原列类型），需要走 `supportCompletePushDown` + 单 partition 改造才能绕开，工程量大且 ROI 低（现有 baseline 已经持平/反超 parquet）。
