from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
# Assuming get_spark_session is in a shared module or defined above
from utils import get_spark_session, KAFKA_BROKERS

# --- Spark Session ---
APP_NAME = "BTC_ZScore"
checkpoint_dir = f"file:///tmp/spark_checkpoints/{APP_NAME}_output"
spark = get_spark_session(APP_NAME)

# --- Constants ---
PRICE_KAFKA_TOPIC = "btc-price"
MOVING_KAFKA_TOPIC = "btc-price-moving"
OUTPUT_KAFKA_TOPIC = "btc-price-zscore"
LATE_DATA_THRESHOLD_PRICE = "15 seconds" # Allow slightly more time for join
LATE_DATA_THRESHOLD_STATS = "15 seconds"

# --- Schemas ---
price_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

stat_info_schema = StructType([
    StructField("window", StringType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("std_price", DoubleType(), True)
])

moving_stats_schema = StructType([
    StructField("timestamp", StringType(), True), # Read ISO string first
    StructField("symbol", StringType(), True),
    StructField("stats", ArrayType(stat_info_schema), True)
])

# --- Read Streams ---
# Price Stream
price_stream_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS).option("subscribe", PRICE_KAFKA_TOPIC).option("startingOffsets", "latest").load()
price_stream = price_stream_raw \
    .select(F.from_json(F.col("value").cast("string"), price_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", LATE_DATA_THRESHOLD_PRICE) \
    .withColumnRenamed("timestamp", "price_ts") \
    .withColumnRenamed("symbol", "price_symbol")

# Moving Stats Stream
stats_stream_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS).option("subscribe", MOVING_KAFKA_TOPIC).option("startingOffsets", "latest").load()
stats_stream = stats_stream_raw \
    .select(F.from_json(F.col("value").cast("string"), moving_stats_schema).alias("data")) \
    .select("data.*") \
    .withColumn("stats_ts", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
    .withWatermark("stats_ts", LATE_DATA_THRESHOLD_STATS) \
    .withColumnRenamed("symbol", "stats_symbol")


# --- Join Streams ---
# Join condition: Match symbol and the timestamp.
# The moving stats timestamp represents the *end* of the window.
# A price timestamp should match a window that *includes* it.
# This join is tricky. Let's assume for now the producer timestamp
# aligns reasonably well with window end times for matching.
# A more robust join might need interval joins or adjustments.
# Simple equality join based on the timestamp field (price_ts == stats_ts)
# and symbol. Watermarking handles state management.

join_condition = F.expr("""
    price_ts = stats_ts AND
    price_symbol = stats_symbol
""")

joined_stream = price_stream.join(
    stats_stream,
    join_condition,
    "inner" # Only process where we have both price and corresponding stats
)

# --- Calculate Z-Score ---
# Explode the stats array and calculate Z-score for each window
zscore_stream = joined_stream \
    .select(
        F.col("price_ts").alias("event_timestamp"),
        F.col("price_symbol").alias("symbol"),
        F.col("price"),
        F.explode("stats").alias("stat_info") # Explode array into multiple rows
    ) \
    .select(
        F.col("event_timestamp"),
        F.col("symbol"),
        F.col("price"),
        F.col("stat_info.window").alias("window"),
        F.col("stat_info.avg_price").alias("avg_price"),
        F.col("stat_info.std_price").alias("std_price")
    ) \
    .withColumn(
        "zscore_price",
        F.when( (F.col("std_price").isNull()) | (F.col("std_price") == 0.0) | F.isnan(F.col("std_price")), 0.0 ) # Handle zero/null/NaN std dev
         .otherwise( (F.col("price") - F.col("avg_price")) / F.col("std_price") )
    )

# --- Format Output ---
# Group back to get the array structure required
output_stream = zscore_stream \
    .select(
        F.col("event_timestamp"),
        F.col("symbol"),
        F.struct( # Create struct for each window's z-score
            F.col("window"),
            F.col("zscore_price")
        ).alias("zscore_data")
    ) \
    .groupBy("event_timestamp", "symbol") \
    .agg(F.collect_list("zscore_data").alias("zscores")) \
    .select(
        F.date_format(F.col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"), # ISO8601 format
        F.col("symbol"),
        F.col("zscores")
    ) \
    .select( # Convert final result to JSON string for Kafka
        F.to_json(
            F.struct(F.col("*")) # Select all columns within the struct
        ).alias("value")
    )


# --- Write to Kafka ---
query = output_stream \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", OUTPUT_KAFKA_TOPIC) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

print(f"Streaming Z-scores to Kafka topic: {OUTPUT_KAFKA_TOPIC}")
query.awaitTermination()

# Clean up Spark Session
spark.stop()