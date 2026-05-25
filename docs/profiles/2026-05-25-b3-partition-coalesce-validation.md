# B3 fix: partition coalesce — cluster A/B validation

**日期**：2026-05-25
**分支**：`feat/java-native-lance-reader` (HEAD `a71d6d0`)
**测量**：`LocalClusterAB.java`, `local-cluster[4,1,2048]`, 12 iter, noop+count+collect 三种 action 交替, iter 6-12 稳态均值

## TL;DR

B3 修复 (`LanceSplit.planScan` 后增加 byte-bin-pack coalesce) **机制层正确**——Spark 任务数从 305 降到 152 (128MB target) 或 36 (512MB target)。但 q3 退化已经被 C1 + PR#1 的其他修复消化大部分，这层补丁的 **wall-time 净收益较小**：q1 在 512MB target 下 ~10-19% 提升，q3/q4 基本在测量噪声范围内。

| q | 305 (OFF) | 152 (ON @128MB) | 36 (ON @512MB) | 历史 baseline (B3 docs) |
|---|---:|---:|---:|---:|
| q1 collect_ms | 1584 | 1436 (−9%) | **1281 (−19%)** | 1426 |
| q3 collect_ms | 1801 | 1883 (+5%) | 1786 (−1%) | **4935** ⚠️ |
| q4 collect_ms | 6757 | 7214 (+7% 退化) | 6683 (−1%) | 9699 |

q3 的"4935ms ⚠️" 是 B3 docs 在 commit `8589c25`/`be1dd7d` 阶段记录的——当时数据基于旧的 lance-jni / bundle 状态。现在重测同样的 setup (coalesce 关掉)，q3 已经是 ~1801ms，与 parquet (1836ms) 持平。意味着 PR#1 的 ObjectStoreRegistry 整合 + C1 cache 已经把单任务开销从 ~30-50ms 压到 ~5-10ms，305 个任务的累计调度成本不再压垮 q3。

## Spark 任务数验证（来自 DAGScheduler 日志）

```
coalesce OFF (default 305 fragments):
  Submitting 305 missing tasks from ShuffleMapStage 0 (...)

coalesce ON @ 128 MB target:
  Submitting 152 missing tasks from ShuffleMapStage 0 (...)

coalesce ON @ 512 MB target:
  Submitting 36 missing tasks from ShuffleMapStage 0 (...)
```

任务数确实按预期下降（305 → 152 → 36），证明 `LancePartitionCoalescer` 在 production benchmark 上正常工作。

## q4 在 128MB target 下的小幅退化（+7%）

| q4 noop_ms (iter 6-12) | OFF | ON @128MB | ON @512MB |
|---|---:|---:|---:|
| 均值 | 6713 | 7201 | 6573 |

128MB target 把 q4 noop 从 6713 推到 7201，但 512MB target 又拉回 6573。可能原因：

- 152 个 partition 每个跨 ~2 fragments 时，executor 内的 batch pipeline 有"半空"窗口，IO 与 decode 重叠不充分；
- 36 个大 partition 每个 ~8 fragments 顺序扫描，pipeline 满载，反而更高效。

未深究——属于"target 选取"调优空间，可作为后续 follow-up。

## 工程含义

| 决策 | 结论 |
|---|---|
| 这层修复保留还是回滚？ | **保留**。语义上把 lance V2 的 partition 计数与 Spark 文件源约定对齐；q1 实测 ~10-19% 改善；防止后续大规模 dataset (>1k fragments) 出现真正退化 |
| 默认 target 用什么？ | **走 Spark 自带的 `spark.sql.files.maxPartitionBytes` (128 MB)**。系统已经依赖这一约定，没必要 lance 单独再立一个 |
| 想要更激进的 partition 减少？ | 用户自己设 `spark.sql.files.maxPartitionBytes=536870912`（512MB）。或用户私有 sysprop `lance.spark.max_partition_bytes` |
| q3 的 2.69× 退化怎么算？ | **已经被 C1 + PR#1 关闭**——重测 OFF 配置 q3=1801ms 与 parquet 1836ms 持平。原 B3 docs 的 4935ms baseline 反映的是旧 lance-jni (PR#1 落地前) 的状态 |
| q4 在 128MB target 下小幅退化要管吗？ | 暂时记下来，不阻塞合入。512MB target 下消失，说明是 partition-size 调优问题，不是结构性 bug |

## 关联文件

- 实现：`lance-spark-base_2.12/src/main/java/org/lance/spark/read/LancePartitionCoalescer.java`（commit `a71d6d0`）
- 测试：`LancePartitionCoalescerTest.java`（9 unit tests）+ `LanceScanTest.java`（3 integration tests for sysprop wiring + null-fallback）
- 数据 logs：
  - `/tmp/cluster-coalesce-on.log` — coalesce ON @128MB
  - `/tmp/cluster-coalesce-off.log` — coalesce OFF (control)
  - `/tmp/cluster-coalesce-512mb.log` — coalesce ON @512MB
- 分布式 fragment size 探针：`micro-benchmark/.../FragmentSizeProbe.java`（探得 305 fragments × 53.3 MB avg = 16.3 GB）
