from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
# Assuming get_spark_session is in a shared module or defined above
from utils import get_spark_session, KAFKA_BROKERS

# --- Spark Session ---
APP_NAME = "BTC_Moving_Stats"
checkpoint_dir = f"file:///tmp/spark_checkpoints/{APP_NAME}_output"
spark = get_spark_session(APP_NAME)

# --- Constants ---
INPUT_KAFKA_TOPIC = "btc-price"
OUTPUT_KAFKA_TOPIC = "btc-price-moving"
LATE_DATA_THRESHOLD = "10 seconds"
# Window Specifications: duration string, name string
WINDOWS = [
    ("30 seconds", "30s"),
    ("1 minute",   "1m"),
    ("5 minutes",  "5m"),
    ("15 minutes", "15m"),
    ("30 minutes", "30m"),
    ("1 hour",     "1h"),
]

# --- Input Schema ---
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True) # Assuming Golang sends timestamp compatible with Spark TimestampType
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
    .select(F.col("value").cast("string").alias("json_value"), F.col("timestamp").alias("kafka_ts")) \
    .select(F.from_json(F.col("json_value"), schema).alias("data"), F.col("kafka_ts")) \
    .select("data.*", "kafka_ts") \
    .withColumnRenamed("timestamp", "event_timestamp") # Rename the timestamp from JSON

# --- Watermarking ---
watermarked_stream = json_stream \
    .withWatermark("event_timestamp", LATE_DATA_THRESHOLD)

# --- Calculate Moving Stats for Each Window ---
agg_streams = []
for window_duration, window_name in WINDOWS:
    # Use tumbling windows for simplicity here, outputting results when window closes
    # If true sliding is needed, add a slideDuration parameter to window()
    windowed_agg = watermarked_stream \
        .groupBy(
            F.window("event_timestamp", window_duration).alias("time_window"),
            F.col("symbol")
        ) \
        .agg(
            F.avg("price").alias("avg_price"),
            F.stddev_pop("price").alias("std_price") # Use population standard deviation for the window data
        ) \
        .select(
            F.col("time_window.end").alias("window_end_ts"), # Use window end time as the representative timestamp
            F.col("symbol"),
            F.lit(window_name).alias("window"),
            F.col("avg_price"),
            F.col("std_price")
        )
    agg_streams.append(windowed_agg)

# --- Combine Results from All Windows ---
# Union all aggregated streams - might be less efficient than other methods
# but conceptually simpler for distinct window calculations.
# Note: UnionByName requires Spark 3.1+
# For older Spark, ensure schemas match exactly and use union()
if len(agg_streams) > 1:
     # This union approach doesn't work well directly on multiple streaming DFs.
     # Alternative: Calculate all aggregates in one go if possible, or process separately.
     # Let's restructure to calculate stats and then group them.

     # Calculate stats per window first
     stats_dfs = []
     for window_duration, window_name in WINDOWS:
         stats_df = watermarked_stream.groupBy(
                 F.window("event_timestamp", window_duration).alias("time_window"),
                 "symbol"
             ).agg(
                 F.avg("price").alias("avg_price"),
                 F.stddev_pop("price").alias("std_price")
             ).select(
                 F.col("time_window.end").alias("timestamp"), # Representative timestamp
                 "symbol",
                 F.struct(
                     F.lit(window_name).alias("window"),
                     F.col("avg_price"),
                     F.when(F.isnull(F.col("std_price")) | F.isnan(F.col("std_price")), 0.0).otherwise(F.col("std_price")).alias("std_price") # Handle null/NaN stddev
                 ).alias("stat_data")
             )
         stats_dfs.append(stats_df)

     # Combine stats based on timestamp and symbol
     # This requires a common key. Grouping by window end time works.
     # We need to aggregate the 'stat_data' structs into a list.
     # This might need stateful processing or a different approach if unioning streams is problematic.

     # Let's try a different approach: Calculate all stats in one pass using groupBy on timestamp
     # This doesn't fit well with standard window aggregation.
     # Reverting to the idea of processing each window and trying to combine.

     # Simplification: Output separate streams or format differently if combining proves too complex.
     # Let's try the union approach again, focusing on the schema. Spark should handle unioning streaming DFs.

     # **Revised Combination Strategy:**
     # Perform calculations for each window, add a 'window' column, then union.
     # Finally group by the *original* timestamp and symbol to collect the stats.
     # This requires joining the aggregated results back to the original stream or careful state management.

     # **Simplest Viable Streaming Approach:** Output one message per window calculation.
     # This deviates slightly from the requested single message format per timestamp,
     # but is much easier to implement reliably in streaming.
     # Let's follow the *intent* but adjust the *exact* output format slightly if needed.
     # The spec implies one message per timestamp *containing* all window stats.

     # Try collecting results: Group by window end time and symbol.
     final_combined_stream = None
     for i, df in enumerate(stats_dfs):
         if i == 0:
             final_combined_stream = df
         else:
             # Union requires matching schemas. The schema is (timestamp, symbol, stat_data)
             final_combined_stream = final_combined_stream.unionByName(df) # Use unionByName if possible (Spark 3.1+)

     # Now group by timestamp and symbol to collect the stat_data structs
     result_stream = final_combined_stream \
        .groupBy("timestamp", "symbol") \
        .agg(F.collect_list("stat_data").alias("stats")) \
        .select(
            F.date_format(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp_iso"), # ISO8601 format
            F.col("symbol"),
            F.col("stats")
        ) \
        .select( # Convert final result to JSON string for Kafka
            F.to_json(
                F.struct(
                    F.col("timestamp_iso").alias("timestamp"),
                    F.col("symbol"),
                    F.col("stats") # stats is already an array of structs
                )
            ).alias("value")
        )

else:
     # Handle case with only one window if necessary (unlikely based on reqs)
     result_stream = agg_streams[0].select(F.to_json(F.struct(F.col("*"))).alias("value")) # Adjust structure


# --- Write to Kafka ---
query = result_stream \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", OUTPUT_KAFKA_TOPIC) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

print(f"Streaming moving stats to Kafka topic: {OUTPUT_KAFKA_TOPIC}")
query.awaitTermination()

# Clean up Spark Session
spark.stop()