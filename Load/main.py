from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
# Assuming get_spark_session is in a shared module or defined above
from utils import get_spark_session, KAFKA_BROKERS, MONGO_URI

# --- Spark Session ---
APP_NAME = "BTC_Load_Mongo"
checkpoint_dir = f"file:///tmp/spark_checkpoints/{APP_NAME}_output"
spark = get_spark_session(APP_NAME, mongo_connection_string=MONGO_URI) # Pass Mongo URI

# --- Constants ---
INPUT_KAFKA_TOPIC = "btc-price-zscore"
MONGO_DB_NAME = MONGO_URI.split("/")[-1].split("?")[0] # Extract DB name from URI

# --- Schema ---
zscore_info_schema = StructType([
    StructField("window", StringType(), True),
    StructField("zscore_price", DoubleType(), True)
])

zscore_schema = StructType([
    StructField("timestamp", StringType(), True), # Read ISO string
    StructField("symbol", StringType(), True),
    StructField("zscores", ArrayType(zscore_info_schema), True)
])

# --- Read from Kafka ---
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", INPUT_KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# --- Parse JSON Data ---
json_stream = raw_stream \
    .select(F.from_json(F.col("value").cast("string"), zscore_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) # Convert back to timestamp if needed


# --- Prepare for MongoDB Write (using foreachBatch) ---
def write_to_mongo(batch_df, batch_id):
    if batch_df.count() == 0:
        return # Skip empty batches

    print(f"Processing batch ID: {batch_id}")

    # Explode the array to write individual records per window
    exploded_df = batch_df \
        .select(
            F.col("event_timestamp"), # Or use the ISO string 'timestamp' directly
            F.col("symbol"),
            F.explode("zscores").alias("zscore_info")
        ) \
        .select(
            F.col("event_timestamp").alias("timestamp"), # Rename for Mongo field
            F.col("symbol"),
            F.col("zscore_info.window").alias("window"),
            F.col("zscore_info.zscore_price").alias("zscore_price")
        ).persist() # Persist before iterating

    try:
        # Get unique window values in this batch
        windows_in_batch = [row.window for row in exploded_df.select("window").distinct().collect()]
        print(f"Windows in batch {batch_id}: {windows_in_batch}")

        for window_val in windows_in_batch:
            collection_name = f"btc-price-zscore-{window_val}"
            print(f"Writing data for window '{window_val}' to collection '{collection_name}'...")

            # Filter data for the current window
            window_df = exploded_df.filter(F.col("window") == window_val) \
                                     .select("timestamp", "symbol", "zscore_price") # Select final fields

            if window_df.count() > 0:
                 # Write the filtered DataFrame to the specific MongoDB collection
                 window_df.write \
                    .format("mongodb") \
                    .mode("append") \
                    .option("database", MONGO_DB_NAME) \
                    .option("collection", collection_name) \
                    .save()
                 print(f"  -> Wrote {window_df.count()} records to {collection_name}")
            else:
                 print(f"  -> No records to write for window {window_val} in this batch.")

    except Exception as e:
        print(f"ERROR processing batch {batch_id}: {e}")
        # Consider adding more robust error handling / logging
    finally:
        exploded_df.unpersist() # Unpersist the exploded DataFrame

# --- Start Streaming Query with foreachBatch ---
query = json_stream \
    .writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

print(f"Streaming data from Kafka topic '{INPUT_KAFKA_TOPIC}' to MongoDB...")
query.awaitTermination()

# Clean up Spark Session
spark.stop()