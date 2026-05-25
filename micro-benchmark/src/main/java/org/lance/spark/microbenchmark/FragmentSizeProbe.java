/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.lance.spark.microbenchmark;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.fragment.DataFile;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;

/**
 * One-off probe: open a Lance dataset URI and dump per-fragment {@link DataFile#getFileSizeBytes()}
 * along with row counts. Used to validate that the bp128 store_sales dataset on MinIO has file
 * sizes recorded (the precondition for {@code LancePartitionCoalescer} to do anything useful).
 *
 * <p>Run with {@code mvn -pl micro-benchmark exec:java
 * -Dexec.mainClass=org.lance.spark.microbenchmark.FragmentSizeProbe}.
 */
public class FragmentSizeProbe {

  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";

  public static void main(String[] args) {
    String uri =
        args.length > 0
            ? args[0]
            : "s3://benchmark/tpcds-sf-100/store_sales_bp128_dispatch.lance";

    java.util.Map<String, String> storage = new java.util.HashMap<>();
    storage.put("aws_access_key_id", ACCESS_KEY);
    storage.put("aws_secret_access_key", SECRET_KEY);
    storage.put("aws_endpoint", MINIO_ENDPOINT);
    storage.put("aws_region", "us-east-1");
    storage.put("aws_virtual_hosted_style_request", "false");
    storage.put("allow_http", "true");

    LanceSparkReadOptions opts =
        LanceSparkReadOptions.builder().datasetUri(uri).storageOptions(storage).build();

    long totalBytes = 0;
    long totalRows = 0;
    int totalFiles = 0;
    int fragmentsWithNullSize = 0;
    int totalFragments = 0;
    long minFragmentBytes = Long.MAX_VALUE;
    long maxFragmentBytes = 0;

    try (Dataset ds = Utils.openDatasetBuilder(opts).build()) {
      for (Fragment f : ds.getFragments()) {
        totalFragments++;
        long fragmentBytes = 0;
        boolean hasNull = false;
        for (DataFile df : f.metadata().getFiles()) {
          totalFiles++;
          Long size = df.getFileSizeBytes();
          if (size == null) {
            hasNull = true;
            break;
          }
          fragmentBytes += size;
        }
        if (hasNull) {
          fragmentsWithNullSize++;
          continue;
        }
        totalBytes += fragmentBytes;
        totalRows += f.metadata().getNumRows();
        minFragmentBytes = Math.min(minFragmentBytes, fragmentBytes);
        maxFragmentBytes = Math.max(maxFragmentBytes, fragmentBytes);
      }
    }

    System.err.println("=== FragmentSizeProbe ===");
    System.err.println("URI: " + uri);
    System.err.println("Total fragments: " + totalFragments);
    System.err.println("Fragments with null fileSizeBytes: " + fragmentsWithNullSize);
    System.err.println("Total data files: " + totalFiles);
    if (totalFragments - fragmentsWithNullSize > 0) {
      double avgBytes =
          (double) totalBytes / (double) (totalFragments - fragmentsWithNullSize);
      double avgRows = (double) totalRows / (double) (totalFragments - fragmentsWithNullSize);
      System.err.println("Total bytes (sized fragments): " + totalBytes);
      System.err.println(String.format("Avg fragment size: %.1f MB", avgBytes / (1024.0 * 1024.0)));
      System.err.println(String.format("Min fragment size: %.1f MB",
          minFragmentBytes / (1024.0 * 1024.0)));
      System.err.println(String.format("Max fragment size: %.1f MB",
          maxFragmentBytes / (1024.0 * 1024.0)));
      System.err.println(String.format("Avg fragment rows: %.0f", avgRows));
      double targetMB = 128.0;
      long projected = (long) Math.ceil(totalBytes / (targetMB * 1024.0 * 1024.0));
      System.err.println(
          String.format(
              "Projected partitions @ %.0f MB target: ~%d (vs current %d)",
              targetMB, projected, totalFragments));
    }
  }
}
