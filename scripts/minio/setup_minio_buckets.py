"""
Script to create local MinIO Buckets
"""

import os
from dotenv import load_dotenv

from minio import Minio
from minio.error import S3Error

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECERET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW")
MINIO_BUCKET_PROCESSED = os.getenv("MINIO_BUCKET_PROCESSED")


def setup_minio_buckets():
    """Create required MinIO buckets for local development"""

    # Initialize MinIO client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECERET_KEY,
        secure=False,
    )

    buckets_to_create = [
        MINIO_BUCKET_RAW,
        MINIO_BUCKET_PROCESSED,
    ]

    for bucket_name in buckets_to_create:
        try:
            # Check if bucket exists
            if not client.bucket_exists(bucket_name):
                # Create bucket
                client.make_bucket(bucket_name)
                print(f"✅ Created bucket: {bucket_name}")
            else:
                print(f"ℹ️  Bucket already exists: {bucket_name}")
        except S3Error as e:
            print(f"❌ Error creating bucket {bucket_name}: {e}")


if __name__ == "__main__":
    setup_minio_buckets()
