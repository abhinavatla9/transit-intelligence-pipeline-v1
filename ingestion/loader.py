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
    except ClientError:
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
        trip = entity.trip_update.trip
        vehicle = entity.trip_update.vehicle
        timestamp = entity.trip_update.timestamp

        for stop_update in entity.trip_update.stop_time_update:
            entities.append({
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "direction_id": trip.direction_id,
                "vehicle_id": vehicle.id,
                "vehicle_label": vehicle.label,
                "stop_id": stop_update.stop_id,
                "arrival_delay": stop_update.arrival.delay if stop_update.HasField("arrival") else None,
                "departure_delay": stop_update.departure.delay if stop_update.HasField("departure") else None,
                "timestamp": timestamp
            })

    
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
