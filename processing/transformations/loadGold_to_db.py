import pandas as pd
import psycopg2
from processing.transformations.convert_to_silver import get_spark_sess
import logging

logger = logging.getLogger(__name__)

GOLD_PATH = "s3a://gold/BART/route_metrics"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "transit_db",
    "user": "transit_user",
    "password": "transit_pass"
}

CREATE_TABLE_CONS = """
CREATE TABLE IF NOT EXISTS marts.route_metrics (
    route_id VARCHAR,
    avg_arrival_delay FLOAT,
    max_arrival_delay FLOAT,
    min_arrival_delay FLOAT,
    delay_variability FLOAT,
    total_records BIGINT,
    calculated_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
"""

def connect_postgres():
    return psycopg2.connect(**DB_CONFIG)

def ensure_table_exists(conn):
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_CONS)
    conn.commit()
    cursor.close()

def load_route_metrics_to_db():
    spark = get_spark_sess()

    df_spark = spark.read.parquet(GOLD_PATH)
    df_pandas = df_spark.toPandas()

    logger.info("gold converted to panadas df")

    conn = connect_postgres()
    ensure_table_exists(conn)

    cursor = conn.cursor()

    cursor.execute("DELETE FROM marts.route_metrics")

    for _,row in df_pandas.iterrows():
        cursor.execute("""
                INSERT INTO marts.route_metrics
                       (route_id, avg_arrival_delay, max_arrival_delay, min_arrival_delay, delay_variability,
                       total_records, calculated_at
                       ) VALUES (
                       %s, %s, %s, %s, %s, %s, %s)""", (
                           row["route_id"],
                           row["avg_arrival_delay"],
                           row["max_arrival_delay"],
                           row["min_arrival_delay"],
                           row["delay_variability"],
                           row["total_records"],
                           row["calculated_at"]
                       ))
        
        conn.commit()
        cursor.close()
        conn.close()

        print("Route metrics loaded to PostgreSQL successfully")
        spark.stop()


