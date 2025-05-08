import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
# Assuming utils.py contains get_spark_session and KAFKA_BROKERS
from utils import get_spark_session, KAFKA_BROKERS

# --- Configuration ---
APP_NAME = "BTC_ZScore"
PRICE_KAFKA_TOPIC = "btc-price"
MOVING_KAFKA_TOPIC = "btc-price-moving"
OUTPUT_KAFKA_TOPIC = "btc-price-zscore"

# Watermarks: Allow some delay. Tune these based on observed behavior and requirements.
# Delay for price stream should be relatively short.
LATE_DATA_THRESHOLD_PRICE = "1 minute"
# Delay for stats stream needs to account for max window duration in moving.py + processing time.
# Start large (e.g., 70 mins for 1h window) and potentially reduce if state grows too large but output appears sooner.
LATE_DATA_THRESHOLD_STATS = "2 minutes"
CHECKPOINT_DIR = f"file:///tmp/spark_checkpoints/{APP_NAME}_output"
# Ensure this matches the format produced by extract.py and moving.py
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

# --- Spark Session ---
# Assumes get_spark_session sets necessary configs like packages, bindAddress, and optionally timeZone='UTC'
spark = get_spark_session(APP_NAME)

# --- Schemas ---
# Schema for raw price data from btc-price topic
price_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True) # Read as string first
])

# Schema for the individual stat objects within the 'stats' array from btc-price-moving
stat_info_schema = StructType([
    StructField("window", StringType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("std_price", DoubleType(), True)
])

# Schema for the moving stats data from btc-price-moving topic
moving_stats_schema = StructType([
    StructField("timestamp", StringType(), True),    # Represents window_end as ISO string
    StructField("window_start", StringType(), True), # Represents window_start as ISO string
    StructField("symbol", StringType(), True),
    StructField("stats", ArrayType(stat_info_schema), True)
])

# --- Read Streams ---

print(f"Reading from Kafka topic: {PRICE_KAFKA_TOPIC}")
# 1. Price Stream
price_stream_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", PRICE_KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

price_stream = price_stream_raw \
    .select(F.from_json(F.col("value").cast("string"), price_schema).alias("data")) \
    .select("data.*") \
    .withColumn("price_ts_parsed", F.to_timestamp(F.col("timestamp"), TIMESTAMP_FORMAT)) \
    .filter(F.col("price_ts_parsed").isNotNull()) \
    .withColumnRenamed("symbol", "price_symbol") \
    .withColumnRenamed("price_ts_parsed", "price_ts") \
    .withWatermark("price_ts", LATE_DATA_THRESHOLD_PRICE) \
    .select("price_symbol", "price", "price_ts")

print(f"Reading from Kafka topic: {MOVING_KAFKA_TOPIC}")
# 2. Moving Stats Stream
stats_stream_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", MOVING_KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

stats_stream = stats_stream_raw \
    .select(F.from_json(F.col("value").cast("string"), moving_stats_schema).alias("data")) \
    .select(
        F.col("data.timestamp").alias("window_end_str"),
        F.col("data.window_start").alias("window_start_str"),
        F.col("data.symbol").alias("stats_symbol"),
        F.col("data.stats").alias("stats")
    ) \
    .withColumn("stats_ts_end", F.to_timestamp(F.col("window_end_str"), TIMESTAMP_FORMAT)) \
    .withColumn("stats_ts_start", F.to_timestamp(F.col("window_start_str"), TIMESTAMP_FORMAT)) \
    .filter(F.col("stats_ts_start").isNotNull() & F.col("stats_ts_end").isNotNull()) \
    .withWatermark("stats_ts_end", LATE_DATA_THRESHOLD_STATS) \
    .select("stats_symbol", "stats", "stats_ts_start", "stats_ts_end")

# --- DEBUG Join Condition ---
# Try joining if timestamps are within, say, 5 seconds (adjust as needed)
# THIS IS NOT THE CORRECT LOGIC FOR THE LAB - JUST FOR DEBUGGING THE JOIN MECHANISM
join_condition = F.expr("""
    price_symbol = stats_symbol AND
    abs(unix_timestamp(price_ts) - unix_timestamp(stats_ts_end)) < 5
""")

print("Performing DEBUG join (checking if timestamps are close)...")
joined_stream = price_stream.join(
    stats_stream,
    join_condition,
    "inner"
)
# print("DEBUG Join performed.")

# # --- DEBUG: Write joined_stream Output to Console ---
# print("Attempting to write DEBUG joined stream to console...")
# query_debug_join = joined_stream \
#     .selectExpr("CAST(price_symbol AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", f"file:///tmp/spark_checkpoints/{APP_NAME}_debug_JOIN_CLOSE_TS") \
#     .start()

# print("Debug JOIN output query started. Waiting for termination...")
# query_debug_join.awaitTermination()


# --- Calculate Z-Score ---
print("Calculating Z-Scores...")
zscore_calculated = joined_stream \
    .select(
        F.col("price_ts").alias("event_timestamp"), # Keep original event time
        F.col("price_symbol").alias("symbol"),
        F.col("price"),
        F.explode("stats").alias("stat_info") # Explode stats array to process each window
    ) \
    .select(
        F.col("event_timestamp"),
        F.col("symbol"),
        F.col("price"),
        F.col("stat_info.window").alias("window"),
        # Ensure stats values are read correctly
        F.col("stat_info.avg_price").cast(DoubleType()).alias("avg_price"),
        F.col("stat_info.std_price").cast(DoubleType()).alias("std_price")
    ) \
    .filter(F.col("avg_price").isNotNull() & F.col("std_price").isNotNull()) \
    .withColumn(
        "zscore_price",
        # Handle std_price being zero or null/NaN to avoid division errors
        F.when( (F.col("std_price") == 0.0) | F.isnan(F.col("std_price")), 0.0 )
         .otherwise( (F.col("price") - F.col("avg_price")) / F.col("std_price") )
    )


# --- Format Output ---
print("Formatting output...")
output_stream = zscore_calculated \
    .select(
        F.col("event_timestamp"),
        F.col("symbol"),
        # Create a struct for each window's z-score info
        F.struct(
            F.col("window"),
            F.col("zscore_price")
        ).alias("zscore_data")
    ) \
    .groupBy("event_timestamp", "symbol") \
    .agg(F.collect_list("zscore_data").alias("zscores")) \
    .select(
        # Format timestamp back to ISO string for JSON output
        F.date_format(F.col("event_timestamp"), TIMESTAMP_FORMAT).alias("timestamp"),
        F.col("symbol"),
        F.col("zscores") # This is the array of z-score structs [{window: "30s", zscore_price: 1.23}, ...]
    ) \
    .select(
        # Convert the final row structure to a single JSON string column named "value"
        F.to_json(
            F.struct(F.col("*")) # Select all columns (timestamp, symbol, zscores) for the struct
        ).alias("value")
    )

# --- Write to Kafka ---
print("Starting Kafka write stream...")
query = output_stream \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", OUTPUT_KAFKA_TOPIC) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

print(f"Streaming Z-scores to Kafka topic: {OUTPUT_KAFKA_TOPIC}")
# Keep the query running until terminated externally (e.g., Ctrl+C)
query.awaitTermination()

# Clean up Spark Session (will only be reached if query terminates gracefully)
print("Query terminated. Stopping Spark session.")
spark.stop()