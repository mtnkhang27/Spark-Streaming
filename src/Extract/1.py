import requests
import json
import time
import logging
import os
import signal
import sys
# **Imported correctly**
from datetime import datetime, timezone
# **Imported correctly**
from confluent_kafka import Producer

# --- Configuration ---
# Looks correct
BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
KAFKA_TOPIC = "btc-price"
FETCH_INTERVAL_SECONDS = 0.1 # Meets "at least once per 100 ms"
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092") # Good use of env var

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# --- Global flag for graceful shutdown ---
run = True # Standard pattern

# --- Shutdown Handler ---
# Correctly handles signals to set run=False
def shutdown_handler(signum, frame):
    global run
    log.info(f"Caught signal {signum}, initiating shutdown...")
    run = False

# --- Kafka Delivery Report Callback ---
# Standard callback for async producer, logs errors. Looks good.
def delivery_report(err, msg):
    if err is not None:
        log.error(f'Message delivery failed: {err}')
    # else: # Debug logging commented out - appropriate
    #     log.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# --- Main Function ---
def main():
    global run

    # --- Kafka Producer Setup ---
    # Correctly initializes producer with bootstrap servers.
    # Includes try/except for fatal initialization errors. Good.
    try:
        producer_conf = {'bootstrap.servers': KAFKA_BROKERS}
        producer = Producer(producer_conf)
        log.info(f"Kafka producer connected to brokers: {KAFKA_BROKERS}")
    except Exception as e:
        log.fatal(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    # --- Register Signal Handlers ---
    # Correctly registers handlers.
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    log.info("Starting price fetch loop...")

    # --- Main Fetch Loop ---
    while run: # Correctly uses the flag
        start_time = time.monotonic() # Good choice for interval timing

        try:
            # 1. Fetch Price
            # Uses correct URL, includes timeout
            response = requests.get(BINANCE_API_URL, timeout=5)
            # **CRITICAL:** Gets timestamp immediately after response using UTC.
            received_timestamp = datetime.now(timezone.utc)
            # Checks for HTTP errors. 
            response.raise_for_status()

            # 2. Validate and Parse JSON
            try:
                data = response.json()
                if not isinstance(data, dict): 
                    raise ValueError("Response is not a JSON object")

                symbol = data.get("symbol")
                price_str = data.get("price")

                # Checks symbol
                if symbol != "BTCUSDT":
                    log.warning(f"Received unexpected symbol: {symbol}. Skipping.")
                    continue

                # Checks for missing price
                if price_str is None:
                     log.warning(f"Price field missing in response: {data}. Skipping.")
                     continue

                # Converts price. (ValueError handled by except block)
                price_float = float(price_str)

            except (json.JSONDecodeError, ValueError, KeyError) as e:
                log.error(f"Error processing Binance response: {e} - Response: {response.text[:200]}")
                continue

            # 3. Prepare Kafka Message
           #Formats timestamp correctly to ISO8601 with millis and 'Z'
            timestamp_iso = received_timestamp.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

            # Creates correct payload structure.
            message_payload = {
                "symbol": symbol,
                "price": price_float,
                "timestamp": timestamp_iso # Uses the correct ISO string
            }
            # Encodes to bytes.
            message_bytes = json.dumps(message_payload).encode('utf-8')

            # 4. Produce to Kafka
            producer.produce(KAFKA_TOPIC,
                             value=message_bytes,
                             key=symbol.encode('utf-8'),
                             callback=delivery_report)

            # Trigger delivery report callbacks. Correct.
            producer.poll(0)

        except requests.exceptions.Timeout:
            log.warning(f"Request to Binance API timed out.")
        except requests.exceptions.RequestException as e:
            log.error(f"Error fetching price from Binance: {e}")
        except Exception as e:
            log.error(f"An unexpected error occurred: {e}", exc_info=True)

        # --- Maintain Fetch Interval ---
        elapsed_time = time.monotonic() - start_time
        sleep_duration = max(0, FETCH_INTERVAL_SECONDS - elapsed_time)
        if run:
            time.sleep(sleep_duration)

    # --- Shutdown Sequence ---
    # Logs exit.
    log.info("Exiting fetch loop.")
    log.info("Flushing Kafka producer...")
    # Correctly flushes producer with timeout.
    # Provides feedback on remaining messages. Good.
    try:
        remaining_messages = producer.flush(timeout=15)
        if remaining_messages > 0:
             log.warning(f"{remaining_messages} messages were potentially not delivered.")
        else:
             log.info("All messages flushed successfully.")
    except Exception as e:
         log.error(f"Error during producer flush: {e}")

    log.info("Producer closed.")

# --- Main Execution Block ---
# Standard Python practice. Correct.
if __name__ == "__main__":
    main()