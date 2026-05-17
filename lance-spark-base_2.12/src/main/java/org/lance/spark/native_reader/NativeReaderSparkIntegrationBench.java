package org.lance.spark.native_reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Benchmark: NativeLancePartitionReader in a Spark-like loop.
 * Reads one fragment from S3, decodes with Java-native reader,
 * consumes batches like Spark would.
 */
public class NativeReaderSparkIntegrationBench {
  public static void main(String[] args) throws Exception {
    String filePath = "s3a://benchmark/tpcds-sf-100/store_sales.lance/data/"
        + "10111110011001111000001116c9f243f8b974c6b80dafd4a1.lance";
    int columnIndex = 10; // ss_quantity
    int batchSize = 4096;

    Configuration conf = new Configuration();
    conf.set("fs.s3a.endpoint", "http://localhost:9000");
    conf.set("fs.s3a.access.key", "minioadmin");
    conf.set("fs.s3a.secret.key", "minioadmin");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");

    org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType()
        .add("ss_quantity", org.apache.spark.sql.types.DataTypes.IntegerType, true);

    // Open file once (simulates page-cache-hot scenario)
    System.out.println("Opening file and reading page data...");
    NativeLancePartitionReader warmupReader =
        new NativeLancePartitionReader(filePath, columnIndex, batchSize, schema, conf);
    warmupReader.close();

    // Benchmark: 144 iterations using same file data (page cache hot)
    int iterations = 144;
    long totalSum = 0;
    long totalRows = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < iterations; i++) {
      NativeLancePartitionReader reader =
          new NativeLancePartitionReader(filePath, columnIndex, batchSize, schema, conf);
      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        int numRows = batch.numRows();
        org.apache.spark.sql.vectorized.ColumnVector col = batch.column(0);
        for (int j = 0; j < numRows; j++) {
          if (!col.isNullAt(j)) {
            totalSum += col.getInt(j);
          }
        }
        totalRows += numRows;
      }
      reader.close();
    }
    long elapsed = System.currentTimeMillis() - start;

    System.out.println("\nResults:");
    System.out.println("  Total rows: " + totalRows);
    System.out.println("  Sum: " + totalSum);
    System.out.println("  Time: " + elapsed + "ms");
    System.out.printf("  Per row: %.1f ns/row%n", elapsed * 1e6 / totalRows);
    System.out.println("\nComparison (single-thread, includes S3 reads):");
    System.out.println("  This (native PartitionReader): " + elapsed + "ms");
    System.out.println("  Raw decode (no I/O, no ColumnVector): 1083ms");
    System.out.println("  Lance JNI (Spark 12 cores):           1900ms");
    System.out.println("  Parquet (Spark 12 cores):              367ms");
  }
}
