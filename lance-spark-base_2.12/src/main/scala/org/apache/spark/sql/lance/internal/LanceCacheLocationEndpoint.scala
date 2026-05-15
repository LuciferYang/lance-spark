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
package org.apache.spark.sql.lance.internal

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorRemoved}
import org.lance.spark.internal.{CacheLocationReport, LanceCacheLocationReporter, LanceCacheLocationTracker}

/**
 * Driver-side RPC endpoint that receives [[CacheLocationReport]] messages from executors and
 * forwards them to [[LanceCacheLocationTracker]]. Also registers a SparkListener to clean up
 * cache location entries when executors are removed.
 *
 * Registered once per SparkContext via [[LanceCacheLocationEndpoint.ensureRegistered()]].
 */
class LanceCacheLocationEndpoint(override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  private val tracker = LanceCacheLocationTracker.getInstance()

  override def receive: PartialFunction[Any, Unit] = {
    case report: CacheLocationReport =>
      tracker.reportCached(report.getFingerprint, report.getExecutorId, report.getHost)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case report: CacheLocationReport =>
      tracker.reportCached(report.getFingerprint, report.getExecutorId, report.getHost)
      context.reply(true)
  }
}

object LanceCacheLocationEndpoint extends Logging {

  @volatile private var registered: Boolean = false

  def ensureRegistered(sc: SparkContext): Unit = {
    if (registered) return
    synchronized {
      if (registered) return
      try {
        val rpcEnv = sc.env.rpcEnv
        rpcEnv.setupEndpoint(
          LanceCacheLocationReporter.ENDPOINT_NAME,
          new LanceCacheLocationEndpoint(rpcEnv))

        sc.addSparkListener(new SparkListener {
          override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
            LanceCacheLocationTracker.getInstance().removeExecutor(event.executorId)
          }
        })

        registered = true
        logInfo("LanceCacheLocationEndpoint registered on driver")
      } catch {
        case t: Throwable =>
          logWarning(s"Failed to register LanceCacheLocationEndpoint: ${t.getMessage}")
      }
    }
  }

  // For testing
  private[internal] def resetForTesting(): Unit = synchronized {
    registered = false
  }
}
