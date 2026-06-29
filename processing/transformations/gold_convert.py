import logging
from pyspark.sql.functions import avg, count, min, max, stddev, current_timestamp
from processing.transformations.convert_to_silver import get_spark_sess, check_silver_bucket_exists

logger = logging.getLogger(__file__)

SILVER_PATH = "s3a://silver/BART/trip_updates"
GOLD_PATH = "s3a://gold/BART/route_metrics"

def check_gold_bucket_exists():
    import boto3
    from botocore.exceptions import ClientError
    client = boto3.client(
        "s3",
        endpoint_url = "http://localhost:9000",
        aws_access_key_id = "minioadmin",
        aws_secret_access_key = "miniopass"
    )
    try:
        client.head_bucket(Bucket="gold")
    except ClientError:
        client.create_bucket(Bucket="gold")

def calculate_route_metrics():
    spark = get_spark_sess()

    df = spark.read.parquet(SILVER_PATH)


    route_metrics = df.groupBy("route_id").agg(
        avg("arrival_delay").alias("avg_arrival_delay"),
        max("arrival_delay").alias("max_arrival_delay"),
        min("arrival_delay").alias("min_arrival_delay"),
        stddev("arrival_delay").alias("delay_variability"),
        count("arrival_delay").alias("total_records")
    )

    route_metrics = route_metrics.withColumn("calculated_at", current_timestamp())

    check_gold_bucket_exists()

    route_metrics.write \
        .mode("overwrite") \
        .parquet(GOLD_PATH)
    
    print(f"Wrote to Gold {route_metrics.count()}")
    spark.stop()