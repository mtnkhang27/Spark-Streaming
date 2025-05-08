import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str, mongo_connection_string: str = None):
    """Creates or gets a Spark Session with Kafka and MongoDB packages."""

    # --- ADJUST THIS LINE ---
    # Use the version matching your Spark installation (3.5.5)
    kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"

    # You might also want to check the latest compatible Mongo connector for Spark 3.5.x
    # As of now, 10.3.0 is the latest, generally compatible with Spark 3.5. Check official docs if unsure.
    mongo_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0" # Example update

    packages = [kafka_pkg]
    if mongo_connection_string:
        packages.append(mongo_pkg)

    spark_builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") # Keep this if needed

    if mongo_connection_string:
        spark_builder = spark_builder.config("spark.mongodb.output.uri", mongo_connection_string)
        spark_builder = spark_builder.config("spark.mongodb.input.uri", mongo_connection_string)


    # Adjust checkpoint path to use file:/// if running locally
    # This should be set in the writeStream options, not necessarily here globally
    # spark_builder = spark_builder.config("spark.sql.streaming.checkpointLocation", f"file:///tmp/spark_checkpoints/{app_name}")

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# --- Environment Variables (example if defined outside function) ---
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/crypto")