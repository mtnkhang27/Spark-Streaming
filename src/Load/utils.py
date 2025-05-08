# utils.py
import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str, mongo_connection_string: str = None):
    spark_builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    if mongo_connection_string:
        spark_builder = spark_builder.config("spark.mongodb.output.uri", mongo_connection_string)
        spark_builder = spark_builder.config("spark.mongodb.input.uri", mongo_connection_string)

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# --- Environment Variables (for connection strings, keep these) ---
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/crypto")