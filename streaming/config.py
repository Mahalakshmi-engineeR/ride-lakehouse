import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trips")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("LAKE_BUCKET", "lake")

BRONZE_PATH = f"s3a://{BUCKET}/bronze/trips"
SILVER_PATH = f"s3a://{BUCKET}/silver/trips"
QUARANTINE_PATH = f"s3a://{BUCKET}/quarantine/trips"
CHECKPOINT_BASE = f"s3a://{BUCKET}/_checkpoints"
