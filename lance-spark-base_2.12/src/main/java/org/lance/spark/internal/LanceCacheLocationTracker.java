/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Driver-side singleton that tracks which executor has cached which fragment. Executor JVMs report
 * cache-miss completions via {@link LanceCacheLocationReporter}, and {@link
 * org.lance.spark.read.LanceScan#planInputPartitions()} queries this tracker to set {@code
 * preferredLocations} on each partition.
 *
 * <p>Location format: {@code executor_{hostname}_{executorId}} — recognized by Spark's DAGScheduler
 * as {@code ExecutorCacheTaskLocation}, enabling PROCESS_LOCAL scheduling.
 *
 * <p>Lifecycle: initialized lazily on first access within a SparkContext. Executor removal is
 * handled by calling {@link #removeExecutor(String)} (typically from a SparkListener).
 */
public final class LanceCacheLocationTracker {
  private static final Logger LOG = LoggerFactory.getLogger(LanceCacheLocationTracker.class);

  private static volatile LanceCacheLocationTracker instance;

  private final ConcurrentHashMap<String, String> cacheLocations = new ConcurrentHashMap<>();

  private LanceCacheLocationTracker() {}

  public static LanceCacheLocationTracker getInstance() {
    LanceCacheLocationTracker local = instance;
    if (local == null) {
      synchronized (LanceCacheLocationTracker.class) {
        local = instance;
        if (local == null) {
          local = new LanceCacheLocationTracker();
          instance = local;
        }
      }
    }
    return local;
  }

  static void resetForTesting() {
    synchronized (LanceCacheLocationTracker.class) {
      if (instance != null) {
        instance.cacheLocations.clear();
      }
      instance = null;
    }
  }

  public void reportCached(String fingerprint, String executorId, String host) {
    String location = "executor_" + host + "_" + executorId;
    cacheLocations.put(fingerprint, location);
    LOG.debug(
        "Cache location registered: fp={} -> {}",
        fingerprint.substring(0, Math.min(12, fingerprint.length())),
        location);
  }

  public String[] getPreferredLocations(String fingerprint) {
    String loc = cacheLocations.get(fingerprint);
    return loc != null ? new String[] {loc} : new String[0];
  }

  public void removeExecutor(String executorId) {
    String suffix = "_" + executorId;
    java.util.concurrent.atomic.AtomicInteger removed =
        new java.util.concurrent.atomic.AtomicInteger();
    cacheLocations
        .values()
        .removeIf(
            v -> {
              if (v.endsWith(suffix)) {
                removed.incrementAndGet();
                return true;
              }
              return false;
            });
    if (removed.get() > 0) {
      LOG.info("Removed {} cache location entries for executor {}", removed.get(), executorId);
    }
  }

  public int size() {
    return cacheLocations.size();
  }
}
