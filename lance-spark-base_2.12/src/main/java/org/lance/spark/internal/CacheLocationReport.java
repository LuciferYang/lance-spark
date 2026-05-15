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

import java.io.Serializable;

/** RPC message sent from executor to driver when a cache miss completes. */
public final class CacheLocationReport implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String fingerprint;
  private final String executorId;
  private final String host;

  CacheLocationReport(String fingerprint, String executorId, String host) {
    this.fingerprint = fingerprint;
    this.executorId = executorId;
    this.host = host;
  }

  public String getFingerprint() {
    return fingerprint;
  }

  public String getExecutorId() {
    return executorId;
  }

  public String getHost() {
    return host;
  }
}
