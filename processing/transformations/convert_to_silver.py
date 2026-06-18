import logging
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, lit, current_timestamp
from pyspark.sql.types import ( StringType, StructField, StructType, IntegerType, ArrayType, LongType)

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "miniopass"
BRONZE_PATH = "s3a://bronze/BART/trip_updates"
SILVER_PATH = "s3a://silver/BART/trip_updates"

HOME = os.path.expanduser("~")
JARS = ",".join([
    f"{HOME}/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar",
    f"{HOME}/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar"
])

def get_spark_sess() -> SparkSession:
    return SparkSession.builder \
    .appName('TransformToSilverLoader') \
    .config("spark.jars", JARS) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

def get_schema() -> StructType:
    entity_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("direction_id", IntegerType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("vehicle_label", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("arrival_delay", IntegerType(), True),
        StructField("departure_delay", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])
    return StructType([
        StructField("feed_type", StringType(), True),
        StructField("agency", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("entity_count", IntegerType(), True),
        StructField("entities", ArrayType(entity_schema), True)
        ])
def check_silver_bucket_exists():
    client = boto3.client(
        "s3",
        endpoint_url = MINIO_ENDPOINT,
        aws_access_key_id = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY
    )
    try:
        client.head_bucket(Bucket="silver")
    except ClientError:
        client.create_bucket(Bucket="silver")


def transform_to_silver(date: str):
    spark = get_spark_sess()

    df = spark.read.schema(get_schema()).json(f"{BRONZE_PATH}/{date}")

    df_expanded = df.select(
        col("feed_type"),
        col("agency"),
        col("ingested_at"),
        explode(col("entities")).alias("entity")
    )

    df_silver = df_expanded.select(
        col("feed_type"),
        col("agency"),
        col("ingested_at"),
        col("entity.trip_id").alias("trip_id"),
        col("entity.route_id").alias("route_id"),
        col("entity.direction_id").alias("direction_id"),
        col("entity.vehicle_id").alias("vehicle_id"),
        col("entity.vehicle_label").alias("vehicle_label"),
        col("entity.stop_id").alias("stop_id"),
        col("entity.arrival_delay").alias("arrival_delay"),
        col("entity.departure_delay").alias("departure_delay"),
        col("entity.timestamp").alias("timestamp"),
        current_timestamp().alias("processed_at"),
        lit(date).alias("partition_date")
    )

    check_silver_bucket_exists()

    df_silver.write \
        .mode("overwrite") \
        .partitionBy("partition_date") \
        .parquet(SILVER_PATH)
    
    logger.info(f"Written {df_silver.count()} records to silver")
    spark.stop()

def check_silver_parquet():
    spark = get_spark_sess()

    df = spark.read.parquet(SILVER_PATH)

    df.printSchema()
    df.show(5)
    print("Total Rows:", df.count())
