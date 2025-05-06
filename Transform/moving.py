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
# --- Restructured approach ---
# Calculate stats per window first
stats_dfs = []
for window_duration, window_name in WINDOWS:
    stats_df = watermarked_stream.groupBy(
            F.window("event_timestamp", window_duration).alias("time_window"),
            "symbol"
        ).agg(
            # Calculate aggregates
            F.avg("price").alias("avg_price"),
            F.stddev_pop("price").alias("std_price")
        ).select(
           # Select necessary fields including start/end times
           F.col("time_window.start").alias("w_start"),
           F.col("time_window.end").alias("w_end"),
           "symbol",
           # Create struct WITH aggregate values
           F.struct(
               F.lit(window_name).alias("window"),
               # --- INCLUDE AGGREGATES IN STRUCT ---
               F.col("avg_price"),
               # Handle null/NaN stddev (important!)
               F.when(F.isnull(F.col("std_price")) | F.isnan(F.col("std_price")), 0.0)
                .otherwise(F.col("std_price")).alias("std_price")
               # --- END INCLUDE ---
           ).alias("stat_data") # Rename the struct
       )
    stats_dfs.append(stats_df) # Add DF for this window duration to the list

# --- Combine Results from All Windows ---
final_combined_stream = None
# Need to union the calculated DFs (stats_dfs)
if stats_dfs: # Check if the list is not empty
    # Use reduce and unionByName for robustness if Spark version allows (3.1+)
    # Requires: from functools import reduce
    # from pyspark.sql import DataFrame
    # final_combined_stream = reduce(DataFrame.unionByName, stats_dfs)

    # Simpler union loop for broader compatibility (requires exact schema match)
    # The schema should be (w_start, w_end, symbol, stat_data) - check this
    final_combined_stream = stats_dfs[0]
    for i in range(1, len(stats_dfs)):
        # Ensure schemas match before union if not using unionByName
        # final_combined_stream.printSchema()
        # stats_dfs[i].printSchema()
        final_combined_stream = final_combined_stream.union(stats_dfs[i])

# --- Group results by window end time and symbol ---
if final_combined_stream is not None: # Proceed only if union was successful
    result_stream = final_combined_stream \
       .groupBy("w_end", "symbol") \
       .agg(
           # Use first() to get one start time - assumes all windows ending
           # at the same time for the same symbol also started at the same
           # effective time relative to their duration (may need adjustment if using sliding windows)
           F.first("w_start").alias("window_start_ts"),
           F.collect_list("stat_data").alias("stats") # Collect the structs
       ) \
       .select(
           # Format timestamps for JSON output
           F.date_format(F.col("w_end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
           F.date_format(F.col("window_start_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
           F.col("symbol"),
           F.col("stats") # The collected list of structs
       ) \
       .select( # Convert final result to JSON string for Kafka
           F.to_json(
               F.struct( # Create the final JSON structure
                   F.col("timestamp"),
                   F.col("window_start"),
                   F.col("symbol"),
                   F.col("stats")
               )
           ).alias("value")
       )
else:
    # Handle case where no windows were processed (e.g., no input data)
    # You might want to log a warning or create an empty stream schema
    print("WARN: No dataframes to combine, result_stream will be empty or undefined.")
    # To avoid errors later, define an empty stream with the expected schema
    output_schema = StructType([StructField("value", StringType(), True)])
    result_stream = spark.createDataFrame(spark.sparkContext.emptyRDD(), output_schema)


# --- Write to Kafka ---
# Check if result_stream is defined before writing
if result_stream is not None:
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
else:
    print("ERROR: result_stream was not created. Stopping.")

# Clean up Spark Session
spark.stop()