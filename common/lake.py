"""Shared helpers for the pure-Python (non-Spark) tools: loader, quality checks, maintenance."""
import os

BUCKET = os.getenv("LAKE_BUCKET", "lake")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "warehouse.duckdb")

TABLES = {
    "bronze": f"s3://{BUCKET}/bronze/trips",
    "silver": f"s3://{BUCKET}/silver/trips",
    "quarantine": f"s3://{BUCKET}/quarantine/trips",
}


def storage_options() -> dict:
    return {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        # MinIO has no conditional-put lock service here; fine for a single-writer demo.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
