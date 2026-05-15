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

import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.RpcEndpointRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor-side reporter that notifies the driver when a cache miss completes. After {@link
 * LanceExecutorCache} successfully writes column files for a fragment, this reporter sends the
 * fragment fingerprint + executor identity to the driver's {@link LanceCacheLocationTracker} via
 * Spark RPC.
 *
 * <p>Fire-and-forget: if the RPC fails (driver unreachable, endpoint not registered), the report is
 * silently dropped. The only consequence is that the next query won't get a preferredLocation hint
 * for this fragment — it will be scheduled normally and may miss the cache.
 */
public final class LanceCacheLocationReporter {
  private static final Logger LOG = LoggerFactory.getLogger(LanceCacheLocationReporter.class);

  public static final String ENDPOINT_NAME = "LanceCacheLocation";

  private static volatile LanceCacheLocationReporter instance;
  private final RpcEndpointRef driverEndpoint;
  private final String executorId;
  private final String host;

  private LanceCacheLocationReporter(
      RpcEndpointRef driverEndpoint, String executorId, String host) {
    this.driverEndpoint = driverEndpoint;
    this.executorId = executorId;
    this.host = host;
  }

  public static LanceCacheLocationReporter getOrCreate() {
    LanceCacheLocationReporter local = instance;
    if (local == null) {
      synchronized (LanceCacheLocationReporter.class) {
        local = instance;
        if (local == null) {
          local = create();
          instance = local;
        }
      }
    }
    return local;
  }

  private static LanceCacheLocationReporter create() {
    try {
      SparkEnv env = SparkEnv.get();
      if (env == null) return null;
      String execId = env.executorId();
      String hostName = env.blockManager().blockManagerId().host();
      // Get driver RPC address from SparkConf (standard pattern for executor→driver RPC)
      String driverHost = env.conf().get("spark.driver.host", "localhost");
      int driverPort = env.conf().getInt("spark.driver.port", 7077);
      org.apache.spark.rpc.RpcAddress driverAddr =
          new org.apache.spark.rpc.RpcAddress(driverHost, driverPort);
      RpcEndpointRef ref = env.rpcEnv().setupEndpointRef(driverAddr, ENDPOINT_NAME);
      return new LanceCacheLocationReporter(ref, execId, hostName);
    } catch (Throwable t) {
      LOG.debug(
          "Failed to create cache location reporter (expected in local mode): {}", t.getMessage());
      return null;
    }
  }

  public void reportCached(String fingerprint) {
    if (driverEndpoint == null) return;
    try {
      driverEndpoint.send(new CacheLocationReport(fingerprint, executorId, host));
    } catch (Throwable t) {
      LOG.debug(
          "Failed to report cache location for fp={}: {}",
          fingerprint.substring(0, Math.min(12, fingerprint.length())),
          t.getMessage());
    }
  }

  static void resetForTesting() {
    synchronized (LanceCacheLocationReporter.class) {
      instance = null;
    }
  }
}
