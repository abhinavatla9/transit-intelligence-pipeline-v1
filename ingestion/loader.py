from datetime import datetime
import json
import logging
from dataclasses import asdict

import boto3
from botocore.exceptions import ClientError
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ADMIN = "minioadmin"
MINIO_PASS = "miniopass"
BRONZE_BUCKET = "bronze"

def get_minio_conn():
    return boto3.client (
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ADMIN,
        aws_secret_access_key=MINIO_PASS
    )

def check_bucket_exists (client, bucket_name: str):
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info(f"bucker {bucket_name} already exists")
    except:
        client.create_bucket(Bucket=bucket_name)
        logger.info(f"bucket {bucket_name} successfully created")

def build_key(feed_type: str):
    now = datetime.now()
    return f"BART/{feed_type}/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.json"

def load_to_bronze(feed: gtfs_realtime_pb2, feed_type: str):
    client = get_minio_conn()
    check_bucket_exists(client, BRONZE_BUCKET)

    key = build_key(feed_type)

    entities = []
    for entity in feed.entity:
        entities.append(str(entity))
    
    payload = {
        "ingested_at": datetime.now().isoformat(),
        "agency": "BART",
        "feed_type": feed_type,
        "entity_count": len(entities),
        "entities": entities
    }
    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )
    logger.info(f" Saved {len(entities)} to {BRONZE_BUCKET} at {key}")
