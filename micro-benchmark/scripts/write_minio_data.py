"""
Writes Lance + Parquet benchmark data locally, then syncs to MinIO via boto3.
Workaround for Lance commit protocol + older MinIO versions that don't support
If-None-Match conditional PUTs.

Run:
  python3 scripts/write_minio_data.py
"""
import os
import io
import shutil
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import lance
import boto3
from botocore.client import Config

NUM_ROWS = int(os.environ.get("NUM_ROWS", 5_000_000))
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "benchmark"


def build_table():
    import numpy as np
    ids = pa.array(np.arange(NUM_ROWS, dtype=np.int64))
    values = pa.array(np.arange(NUM_ROWS, dtype=np.float64) * 2.718)
    # String column: build via arrow directly from int cast to bytes for speed on large N
    names = pa.array(np.arange(NUM_ROWS).astype(str).tolist(), type=pa.string())
    return pa.Table.from_arrays([ids, values, names], names=["id", "value", "name"])


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_tree(local_dir: str, bucket: str, key_prefix: str, s3):
    count = 0
    total_bytes = 0
    for root, _, files in os.walk(local_dir):
        for f in files:
            local_path = os.path.join(root, f)
            rel = os.path.relpath(local_path, local_dir).replace(os.sep, "/")
            key = f"{key_prefix}/{rel}" if key_prefix else rel
            size = os.path.getsize(local_path)
            s3.upload_file(local_path, bucket, key)
            count += 1
            total_bytes += size
    return count, total_bytes


def delete_prefix(bucket: str, prefix: str, s3):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            keys.append({"Key": obj["Key"]})
    if not keys:
        return 0
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[i : i + 1000]})
    return len(keys)


def main():
    tmp = tempfile.mkdtemp(prefix="lance_minio_")
    try:
        print(f"Generating {NUM_ROWS} rows...")
        tbl = build_table()

        lance_local = os.path.join(tmp, "numeric.lance")
        print(f"Writing Lance locally to {lance_local} ...")
        lance.write_dataset(tbl, lance_local, mode="create")
        local_ds = lance.dataset(lance_local)
        print(f"Lance local verify: {local_ds.count_rows()} rows")

        parquet_local = os.path.join(tmp, "numeric.parquet")
        print(f"Writing Parquet locally to {parquet_local} ...")
        pq.write_table(tbl, parquet_local, compression="snappy")

        s3 = s3_client()

        print("Cleaning MinIO destination paths...")
        deleted = delete_prefix(BUCKET, "numeric.lance/", s3)
        print(f"  Deleted {deleted} existing numeric.lance/* objects")
        deleted = delete_prefix(BUCKET, "numeric.parquet", s3)
        print(f"  Deleted {deleted} existing numeric.parquet objects")

        print("Uploading Lance tree to MinIO...")
        n, b = upload_tree(lance_local, BUCKET, "numeric.lance", s3)
        print(f"  Uploaded {n} files, {b:,} bytes")

        print("Uploading Parquet to MinIO...")
        s3.upload_file(parquet_local, BUCKET, "numeric.parquet")
        print(f"  Uploaded {os.path.getsize(parquet_local):,} bytes")

        print("Verifying Lance read from MinIO...")
        storage_options = {
            "aws_access_key_id": ACCESS_KEY,
            "aws_secret_access_key": SECRET_KEY,
            "aws_endpoint": MINIO_ENDPOINT,
            "aws_virtual_hosted_style_request": "false",
            "aws_region": "us-east-1",
            "allow_http": "true",
        }
        ds = lance.dataset(
            f"s3://{BUCKET}/numeric.lance", storage_options=storage_options
        )
        print(f"  Lance rows on MinIO: {ds.count_rows()}")

        print("Data setup complete.")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
