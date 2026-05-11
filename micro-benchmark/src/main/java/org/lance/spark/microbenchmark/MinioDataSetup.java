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
package org.lance.spark.microbenchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Writes benchmark data to MinIO (local S3) for latency simulation experiments.
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>MinIO running on localhost:9000 (user: minioadmin/minioadmin)</li>
 *   <li>Bucket "benchmark" created</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   cd micro-benchmark
 *   mvn compile -q
 *   java -Xms4g -Xmx4g \
 *     --add-opens=java.base/java.lang=ALL-UNNAMED \
 *     --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
 *     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
 *     --add-opens=java.base/java.io=ALL-UNNAMED \
 *     --add-opens=java.base/java.net=ALL-UNNAMED \
 *     --add-opens=java.base/java.nio=ALL-UNNAMED \
 *     --add-opens=java.base/java.util=ALL-UNNAMED \
 *     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
 *     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
 *     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *     --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
 *     --add-opens=java.base/sun.security.action=ALL-UNNAMED \
 *     --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
 *     -Djdk.reflect.useDirectMethodHandle=false \
 *     -Dio.netty.tryReflectionSetAccessible=true \
 *     -cp "target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)" \
 *     org.lance.spark.microbenchmark.MinioDataSetup
 * </pre>
 */
public class MinioDataSetup {

  private static final int NUM_ROWS = 5_000_000;
  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";
  private static final String BUCKET = "s3://benchmark";

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
        .appName("minio-data-setup")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        // Lance V2 write path reads storage options from the catalog namespace, not
        // df.write().option(...) — without this the Rust side fails with CredentialsNotLoaded.
        .config("spark.sql.catalog.lance_default",
            "org.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance_default.aws_access_key_id", ACCESS_KEY)
        .config("spark.sql.catalog.lance_default.aws_secret_access_key", SECRET_KEY)
        .config("spark.sql.catalog.lance_default.aws_endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.lance_default.aws_region", "us-east-1")
        .config("spark.sql.catalog.lance_default.aws_virtual_hosted_style_request", "false")
        .config("spark.sql.catalog.lance_default.allow_http", "true")
        .getOrCreate();

    System.out.println("Generating " + NUM_ROWS + " rows...");
    Dataset<Row> df = spark.sql(
        "SELECT cast(id as long) as id, "
            + "cast(id * 2.718 as double) as value, "
            + "cast(id as string) as name "
            + "FROM range(0, " + NUM_ROWS + ")");

    // Write Lance format directly to MinIO
    System.out.println("Writing Lance table to MinIO...");
    df.write()
        .format("lance")
        .option("aws_access_key_id", ACCESS_KEY)
        .option("aws_secret_access_key", SECRET_KEY)
        .option("aws_endpoint", MINIO_ENDPOINT)
        .option("aws_virtual_hosted_style_request", "false")
        .option("aws_region", "us-east-1")
        .option("allow_http", "true")
        .mode(SaveMode.ErrorIfExists)
        .save(BUCKET + "/numeric.lance");

    // Write Parquet to MinIO for comparison
    System.out.println("Writing Parquet table to MinIO...");
    df.write()
        .format("parquet")
        .option("compression", "snappy")
        .mode(SaveMode.Overwrite)
        .save(BUCKET + "/numeric.parquet");

    // Verify
    long lanceCount = spark.read()
        .format("lance")
        .option("aws_access_key_id", ACCESS_KEY)
        .option("aws_secret_access_key", SECRET_KEY)
        .option("aws_endpoint", MINIO_ENDPOINT)
        .option("aws_virtual_hosted_style_request", "false")
        .option("aws_region", "us-east-1")
        .option("allow_http", "true")
        .load(BUCKET + "/numeric.lance")
        .count();

    long parquetCount = spark.read()
        .format("parquet")
        .load(BUCKET + "/numeric.parquet")
        .count();

    System.out.println("Lance rows: " + lanceCount);
    System.out.println("Parquet rows: " + parquetCount);
    System.out.println("Data setup complete.");

    spark.stop();
  }
}
