# Lance Executor Disk Cache 实施进度

**状态**：✅ **V3 per-column cache 完成 + 103 TPC-DS 实测 -28%**
**设计文档**：[`lance-executor-cache-design.md`](./lance-executor-cache-design.md)

---

## 最终结果（2026-05-14）

### 103 TPC-DS SF-100 benchmark（local-cluster 8×1-core）

| pass | wall (s) | vs baseline |
|---|---|---|
| baseline (cache OFF) | 4240 | — |
| cold (cache ON, fresh) | 3050 | **-28%** |

103/103 queries PASS，0 failures。

### 设计演进

| 版本 | 描述 | 跨 query hit rate | 结论 |
|---|---|---|---|
| V1 | post-filter cache（filter 进 key） | 0.1% | killed |
| V2 | pre-filter, per-projection | ~42% (local) | 有正确性 bug |
| **V3** | pre-filter, per-column | 高（partial hit） | **最终方案** |

---

## 代码

| 文件 | 说明 |
|---|---|
| `LanceExecutorCache.java` | per-column 目录结构 + getOrLoadColumns + LRU eviction |
| `LanceExecutorCacheKey.java` | key = (uri, version, fragId, batchSize, auth) |
| `ColumnAssemblingArrowReader.java` | N 个单列 reader → 多列 VectorSchemaRoot |
| `LanceFragmentScanner.java` | getArrowReader() 集成 |
| `LanceScanBuilder.java` | pushFilters() cache ON 时 early return |
| `LanceExecutorCacheTest.java` | 7 tests（miss/hit/partial/eviction/stale/failure/metrics）|
| `benchmark/scripts/bench-103-tpcds-cache.sh` | local-cluster A/B bench 脚本 |

---

## 已知限制 & 后续方向

1. **调度稀释**：executor 数增加时 hit rate 下降。候选：partition coalesce by hash。
2. **miss 路径列级 scan**：当前 partial miss 时 loader 返回全部列，多余列丢弃。可优化为只 scan miss 列。
3. **eviction/writer race**：极端并发下可能删除正在读的文件。窗口极小。

---

## 关键 bug 修复记录

### pushFilters 正确性 bug（2026-05-14）

**症状**：q90 DIVIDE_BY_ZERO（cache ON 时 pmc=0）。

**根因**：`pushFilters()` 里的 cache guard 条件包含 `readOptions.getVersion() != null`，但 Spark 调用 `pushFilters()` 时 version 还没被 pin（pin 发生在后面的 `build()` 里）。Guard 永远不生效 → filter 照常 push 到 Lance → cache 存的是 filtered 数据 → 跨 query 命中时返回错误数据。

**修复**：去掉 version 检查，只保留 `isEnabled() && isDatasetCacheEnabled()`。同时 `pushedFilters = new Filter[0]` 防止 stale 值。
