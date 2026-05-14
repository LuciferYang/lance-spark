# Lance Executor Disk Cache 设计文档

**状态**：✅ 已实施（V3 per-column）
**日期**：2026-05-14
**基于数据**：
- 103 TPC-DS SF-100 实测：baseline 4240s → cache ON cold 3050s（-28%）
- 8 executor × 1 core local-cluster 模式

---

## 1 · 核心决策

| 维度 | 决策 |
|---|---|
| Cache 层 | lance-spark（不动 lance-core）|
| 拦截点 | `LanceFragmentScanner.getArrowReader()` |
| Cache key | `(datasetUri, pinnedVersion, fragmentId, batchSize, authOptsDigest)` — **不含列投影、不含 filter** |
| Cache value | 每列独立的 Arrow IPC stream 文件 |
| 目录结构 | `{cacheDir}/{fingerprint}/{colName}.arrow` |
| 存储位置 | env `LANCE_EXEC_CACHE_DIR`（默认 `spark.local.dir/lance-cache`）|
| 每 executor 容量 | 独立预算，env `LANCE_EXEC_CACHE_DISK_LIMIT_GB`（默认 30 GB）|
| 淘汰策略 | LRU（按最近访问时间），淘汰单位 = 整个 fragment 目录 |
| 并发 | per-fingerprint `ReentrantLock`，eviction 在 lock 外执行 |
| Spark 调度 | 不动。每 executor 自管，依赖 Spark round-robin 调度的稳定性 |
| Filter pushdown | cache ON 时 **禁用**（`pushFilters()` early return）。Spark Filter 算子负责过滤 |
| 部分命中 | ✅ query [a,b,c] 后 query [b,c,d] → b,c 从 cache 读，只 decode d |
| 失效 | version 变化自然隔离（pinnedVersion 在 key 里）；无 TTL |
| Crash 恢复 | 启动时 `rebuildIndex()` 扫子目录重建 LRU；删除 `.tmp` 文件 |

---

## 2 · 架构概图

```
Spark Task (executor JVM)
  ↓
LanceFragmentScanner.getArrowReader()
  ↓
┌─────────────────────────────────────────────────────┐
│  LanceExecutorCache.getOrLoadColumns(               │
│    key = (uri, version, fragId, batchSize, auth),   │
│    requestedColumns = [a, b, c],                    │
│    loader = missCols -> scanner.scanBatches()       │
│  )                                                   │
│                                                      │
│  全 hit  → 打开 N 个 {col}.arrow 文件               │
│          → ColumnAssemblingArrowReader 拼装          │
│          → 返回多列 ArrowReader                      │
│                                                      │
│  部分 hit → hit 列从 cache 读                        │
│           → miss 列调 loader decode + 写入 cache     │
│           → 拼装返回                                 │
│                                                      │
│  全 miss → 调 loader decode 全部列                   │
│          → 逐列写入 {col}.tmp → atomic rename .arrow │
│          → 拼装返回                                  │
└─────────────────────────────────────────────────────┘
  ↓
Spark Filter 算子 (WHERE clause)
  ↓
Spark BatchScanExec consumes batches
```

---

## 3 · 关键文件

| 文件 | 职责 |
|---|---|
| `LanceExecutorCache.java` | 单例 cache 管理：getOrLoadColumns、LRU、eviction、metrics |
| `LanceExecutorCacheKey.java` | 不可变 cache key + SHA-256 fingerprint（构造时计算一次）|
| `ColumnAssemblingArrowReader.java` | 拼装 N 个单列 ArrowReader 为多列 VectorSchemaRoot |
| `LanceFragmentScanner.java` | 集成点：getArrowReader() 调用 cache API |
| `LanceScanBuilder.java` | pushFilters() 在 cache ON 时 early return |

---

## 4 · 配置

| Env 变量 | 默认值 | 说明 |
|---|---|---|
| `LANCE_EXEC_CACHE_ENABLED` | `false` | 总开关（opt-in）|
| `LANCE_EXEC_CACHE_DIR` | `spark.local.dir/lance-cache` | 磁盘根目录 |
| `LANCE_EXEC_CACHE_DISK_LIMIT_GB` | `30` | 每 executor 磁盘上限 |

local-cluster / standalone 模式下需同时设 `spark.executorEnv.LANCE_EXEC_CACHE_*`。

---

## 5 · 性能数据

### 103 TPC-DS SF-100（local-cluster 8×1-core，16 GB driver，8 GB/executor）

| pass | wall (s) | vs baseline |
|---|---|---|
| baseline (cache OFF) | 4240 | — |
| cold (cache ON, fresh) | 3050 | **-28%** |

### 设计演进

| 版本 | cache key 含 filter? | cache key 含 cols? | 跨 query hit rate | 结论 |
|---|---|---|---|---|
| V1 (post-filter) | ✅ | ✅ | 0.1% | killed |
| V2 (pre-filter, per-projection) | ❌ | ✅ | ~42% (local) | 有正确性 bug |
| **V3 (pre-filter, per-column)** | ❌ | ❌ | 高（partial hit） | **当前** |

---

## 6 · 已知限制

1. **调度稀释**：8+ executor 下同一 fragment 可能落在不同 executor，降低 hit rate。候选方案：partition coalesce by hash（未实施）。
2. **miss 路径不做列级 scan**：partial miss 时 loader 返回全部投影列（不只 miss 列），多余列被丢弃。优化空间：构造只含 miss 列的 ScanOptions。
3. **eviction/writer race**：eviction 不持有 per-key lock，极端并发下可能删除正在被读的文件。窗口极小，TPC-DS 工况下未触发。

---

## 7 · 排除的方向

| 方向 | 原因 |
|---|---|
| RocksDB 存储 | 大 value (10-100 MB) 不适合 LSM；写放大 + compaction 抖动 |
| 内存 cache | 用户明确排除（内存不足） |
| 全列存 + 读时裁剪 | hit 路径读多余数据；miss 路径 decode 全列代价大 |
| Consistent hash 调度 | 用户否决（反模式 + Spark 只是尽量调度）|
| 格式级改造 | 用户排除 |
| 动 lance-core | 全在 lance-spark 完成 |
