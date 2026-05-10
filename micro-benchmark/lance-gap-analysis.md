# Lance vs Parquet: Where the Remaining Gap Lives (100ms RTT)

Profiled with async-profiler 4.4 wall-clock mode via `MinioOneShotRead`, one-shot `sum(id)`
prune query, minio + toxiproxy 100ms RTT latency, cache=true, local[*].

| Metric | Lance prune | Parquet prune |
|---|---|---|
| Wall elapsed | **3198 ms** | **1897 ms** |
| Task-thread total wall samples | 16,492 | 16,021 |
| Task-thread *on-CPU* | 3.3% | **81%** |
| Task-thread *waiting* (cvwait/trap/park) | **92%** | 19% |
| S3 GETs | 632 | 74 |
| S3 HEADs | 1 | 51 |
| S3 request waves | 4 x ~156 bursts | 4 x ~12 bursts |

## Headline finding

The gap is not CPU and not "Lance overhead" in any per-call sense. It's **I/O-compute
overlap**. Parquet task threads spend **81% of their time decoding and aggregating**
while S3A prefetches future data in the background. Lance task threads spend **92%
of their time parked on `__psynch_cvwait`** inside `ArrowArrayStream.getNext` → JNI →
Rust `BlockingDataset` → `tokio::park` → `Condvar::wait_until_internal`.

Lance's Arrow-C-Data pull model is synchronous from the Spark task's point of view:
the task calls `getNext`, parks, waits for Tokio to finish a batch, wakes, processes,
calls `getNext` again. There is no overlap between "Spark doing CPU work" and "Lance
fetching the next batch over S3".

## Two sub-components

### 1. Per-batch serialization cost (dominant, ~2 sec of the 3.2 sec elapsed)

Task-thread stack frequency (wall samples, top leaves):
```
 14,869  __psynch_cvwait         [parked inside ArrowArrayStream.getNext chain]
    240  __psynch_mutexwait
    113  hashAgg_doAggregateWithoutKey_0$
    105  LanceArrowColumnVector.isNullAt
     63  ArrowBuf.refCnt
     42  ArrowBuf.checkIndexD
```

JNI → Rust call path: `ArrowArrayStream.getNext` (10,191 samples) →
`JniWrapper.getNextArrayStream` (10,335) → `arrow_array::ffi_stream::get_next` (10,323)
→ `RecordBatchIteratorAdaptor::next` (10,315) → `tokio::runtime::park::Inner::park`
(10,270) → `parking_lot::Condvar::wait_until_internal` (10,270) → `__psynch_cvwait`.

### 2. Cache-loader fanout (minor, ~200–300 ms, observed only on first fragment)

`LanceFragmentScanner.create` → 4,000 task-thread samples, of which 3,882 pass through
`LanceDatasetCache`. With 48 fragments and N concurrent task threads, the first fragment
misses the cache and does a Dataset.open; the other N-1 threads block on the
`ConcurrentHashMap.computeIfAbsent` lock while that load runs. The resulting thundering
herd shows up in the profile but is quickly amortized (47 of 48 fragments are cache
hits).

## Request-wave anatomy

Request arrival rate over time, per 100 ms bucket:

```
Lance (last req @ 5100ms, 632 total):
   0-   99ms:    1       [setup probes]
 900-  999ms:    1
1200- 1299ms:    1
1600- 1699ms:    1
2400- 2499ms:    1
2700- 2799ms:    1
3100- 3199ms:    1
4000- 4099ms:    1
4100- 4199ms:   12       ← wave 1 start
4200- 4299ms:  144       ← wave 1 burst
4500- 4599ms:   12       ← wave 2 start
4600- 4699ms:  144       ← wave 2 burst
4800- 4899ms:   11
4900- 4999ms:  144       ← wave 3 burst
5000- 5099ms:   12
5100- 5199ms:  144       ← wave 4 burst

Parquet (last req @ 2800ms, 74 total):
   0-   99ms:    1
 100-  199ms:    1
1500- 1599ms:   12
1600- 1699ms:   12
2000- 2099ms:   12
2500- 2599ms:   12
2600- 2699ms:    9
2700- 2799ms:    5
2800- 2899ms:   10
```

Lance issues 12+144 = 156 requests per wave, across 48 fragments = 3.25 GETs per
fragment per wave, 4 waves = 13 GETs per fragment total. Between waves there are gaps
of 300–400 ms during which nothing is in flight — this is where the task threads are
parked waiting for the JNI/Tokio handoff.

## Open hypotheses to close the gap

A. **Overlap I/O with compute at the Spark ↔ Lance boundary.**
   The Arrow C Data Interface pull model enforces synchronous handoff. A prefetch-ahead
   wrapper (background thread pulls next batch while task processes current) would
   move us from 92% wait → closer to Parquet's 19% wait profile. Would need to live
   in `LanceFragmentColumnarBatchScanner` (Java) or `RecordBatchIteratorAdaptor` (Rust).

   **Design constraint**: `ArrowReader` reuses a single `VectorSchemaRoot` across
   `loadNextBatch` calls. The background thread cannot call `loadNextBatch` until the
   task thread is done reading the previous batch's buffers, or the reused root's
   memory would be overwritten mid-read. This means a naive Java wrapper can only
   pipeline the I/O *wait* for the next batch once the task thread has "released"
   the current one — which is roughly what Lance Rust's `batch_readahead` already does
   internally. Net gain from this wrapper is likely small unless we also fix the
   producer side (B/C).

B. **Reduce per-fragment GET count.**
   13 GETs/fragment at 100 ms RTT × 4 serial rounds = 400 ms pure latency. Parquet
   achieves effectively ~2 rounds via S3A prefetch/coalesce. If Lance `block_size` or
   `io_buffer_size` merges adjacent ranges more aggressively, the wave count drops.

C. **Widen per-task readahead — but via the correct Rust-side knob.**
   `batch_readahead` is **misleadingly named** for v2 scans. From
   `rust/lance/src/io/exec/scan.rs:335`, v2 applies `try_buffered(get_num_compute_intensive_cpus())`
   for *CPU decode* parallelism; `batch_readahead` is mainly a v1 artifact
   (`try_buffered(config.batch_readahead)` at scan.rs:404).

   The actual v2 I/O-concurrency levers are:

   | Rust knob                | Default (cloud)                        | Env var                                  | Currently exposed via Spark? |
   |--------------------------|----------------------------------------|------------------------------------------|------------------------------|
   | `io_parallelism`         | 64 (`DEFAULT_CLOUD_IO_PARALLELISM`)    | `LANCE_IO_THREADS`                       | No                           |
   | `io_buffer_size`         | 2 GiB (`DEFAULT_IO_BUFFER_SIZE_VALUE`) | `LANCE_DEFAULT_IO_BUFFER_SIZE`           | No                           |
   | `fragment_readahead`     | 4 (`LEGACY_DEFAULT_FRAGMENT_READAHEAD`)| `LANCE_DEFAULT_FRAGMENT_READAHEAD`       | No                           |
   | `batch_readahead`        | ~#CPU                                  | (via Spark opt `batch_readahead`)        | **Yes** — but wrong knob     |

   Lance Spark per-fragment scanning means `fragment_readahead` is inert (each
   `fragment.newScan()` sees exactly one fragment), so widening it wouldn't help.
   The remaining viable levers are `io_parallelism` and `io_buffer_size`; both
   are well above what the 4-wave observation actually consumes (~3 concurrent
   GETs/fragment), which suggests the bottleneck isn't *capacity* — it's *pipelining*
   between metadata-decode and next-range-planning inside the Lance v2 scheduler.

## Next experiments (small → big)

1. **Confirm v1 vs v2**: dump `is_v2_scan` during the prune query so we know which
   code path above actually applies. This is a one-line log add in
   `LanceStream::try_new`. If we're on v1, `batch_readahead` does control I/O
   concurrency and the existing sweep data should have shown improvement — if it
   didn't, that's evidence the bottleneck is elsewhere.
2. **Bump `LANCE_DEFAULT_IO_BUFFER_SIZE` env var** to 4 GiB at the executor and re-run.
   If waves change pattern, the scheduler's back-pressure was a factor.
3. **Set `LANCE_IO_THREADS=128`** and re-run. If GETs/wave rises, scheduler
   capacity was the bottleneck. If not, we're pipeline-bound.
4. **Prototype Option A wrapper** regardless: even a small win is real, and the
   wrapper also removes the bursty park/wake cycle from task threads (useful for
   Spark's scheduler latency metrics).

## Artifacts

```
/tmp/oneshot-lance-prune-wall-1778355058809.{jfr,html,collapsed}
/tmp/oneshot-parquet-prune-wall-1778355284319.{jfr,html,collapsed}
/tmp/trace-lance-prune-proxy-cachetrue-profwall-20260510-033051.jsonl
/tmp/trace-parquet-prune-proxy-cachetrue-profwall-20260510-033438.jsonl
```
