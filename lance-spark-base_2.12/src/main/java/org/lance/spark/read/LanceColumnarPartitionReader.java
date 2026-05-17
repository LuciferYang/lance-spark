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
package org.lance.spark.read;

import org.lance.spark.internal.LanceFragmentColumnarBatchScanner;
import org.lance.spark.native_reader.LanceMiniBlockDecoder;
import org.lance.spark.native_reader.NativeLanceMultiColReader;
import org.lance.spark.read.metric.LanceReadMetricsTracker;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class LanceColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceInputPartition inputPartition;
  private int fragmentIndex;
  private LanceFragmentColumnarBatchScanner fragmentReader;
  private NativeLanceMultiColReader nativeReader;
  private ColumnarBatch currentBatch;
  private final LanceReadMetricsTracker metricsTracker = new LanceReadMetricsTracker();
  private final boolean useNativeReader;

  public LanceColumnarPartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.fragmentIndex = 0;
    this.useNativeReader = shouldUseNativeReader(inputPartition);
  }

  @Override
  public boolean next() throws IOException {
    if (useNativeReader) {
      return nextNative();
    }
    return nextJni();
  }

  private boolean nextNative() throws IOException {
    if (nativeReader == null) {
      // Initialize native reader for current fragment
      LanceSplit split = inputPartition.getLanceSplit();
      if (split.getDataFilePaths() == null || split.getDataFilePaths().isEmpty()) {
        return nextJni(); // fallback
      }
      String dataFilePath = buildFullDataFilePath(split.getDataFilePaths().get(0));
      int[] colIndices = getColumnIndices();

      org.apache.hadoop.conf.Configuration conf = buildHadoopConf();
      nativeReader =
          new NativeLanceMultiColReader(
              dataFilePath,
              colIndices,
              inputPartition.getSchema(),
              inputPartition.getReadOptions().getBatchSize(),
              conf);
    }
    if (nativeReader.next()) {
      currentBatch = nativeReader.get();
      metricsTracker.addNumBatchesLoaded(1);
      metricsTracker.addNumRowsScanned(currentBatch.numRows());
      return true;
    }
    return false;
  }

  private boolean nextJni() throws IOException {
    if (loadNextBatchFromCurrentReader()) {
      return true;
    }
    while (fragmentIndex < inputPartition.getLanceSplit().getFragments().size()) {
      if (fragmentReader != null) {
        fragmentReader.close();
      }
      fragmentReader =
          LanceFragmentColumnarBatchScanner.create(
              inputPartition.getLanceSplit().getFragments().get(fragmentIndex), inputPartition);
      fragmentIndex++;
      metricsTracker.addNumFragmentsScanned(1);
      metricsTracker.addDatasetOpenTimeNs(fragmentReader.getDatasetOpenTimeNs());
      metricsTracker.addScannerCreateTimeNs(fragmentReader.getScannerCreateTimeNs());
      if (loadNextBatchFromCurrentReader()) {
        return true;
      }
    }
    return false;
  }

  private boolean loadNextBatchFromCurrentReader() throws IOException {
    if (fragmentReader == null) {
      return false;
    }
    if (fragmentReader.loadNextBatch()) {
      currentBatch = fragmentReader.getCurrentBatch();
      metricsTracker.addNumBatchesLoaded(1);
      metricsTracker.addNumRowsScanned(currentBatch.numRows());
      metricsTracker.addBatchLoadTimeNs(fragmentReader.getLastBatchLoadTimeNs());
      return true;
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return metricsTracker.currentMetricsValues();
  }

  @Override
  public void close() throws IOException {
    if (nativeReader != null) {
      nativeReader.close();
    }
    if (fragmentReader != null) {
      try {
        fragmentReader.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private static boolean shouldUseNativeReader(LanceInputPartition partition) {
    // Use native reader when all projected columns are supported types
    if (partition.getReadOptions().isUseNativeReader()) {
      for (StructField field : partition.getSchema().fields()) {
        if (!LanceMiniBlockDecoder.isSupported(field.dataType())) {
          return false;
        }
      }
      return partition.getLanceSplit().getDataFilePaths() != null
          && !partition.getLanceSplit().getDataFilePaths().isEmpty();
    }
    return false;
  }

  private String buildFullDataFilePath(String relativeDataFilePath) {
    String datasetUri = inputPartition.getReadOptions().getDatasetUri();
    // Convert lance:// or s3:// URI to s3a:// for Hadoop
    String base = datasetUri.replace("s3://", "s3a://");
    if (!base.endsWith("/")) base += "/";
    return base + "data/" + relativeDataFilePath;
  }

  private int[] getColumnIndices() {
    // For now, use sequential indices matching the schema
    StructField[] fields = inputPartition.getSchema().fields();
    int[] indices = new int[fields.length];
    for (int i = 0; i < fields.length; i++) {
      indices[i] = i; // TODO: map schema field to actual column index in data file
    }
    return indices;
  }

  private org.apache.hadoop.conf.Configuration buildHadoopConf() {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    // TODO: get these from SparkSession hadoop conf
    conf.set("fs.s3a.endpoint", System.getenv().getOrDefault("AWS_ENDPOINT", ""));
    conf.set("fs.s3a.access.key", System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", ""));
    conf.set("fs.s3a.secret.key", System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", ""));
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");
    return conf;
  }
}
