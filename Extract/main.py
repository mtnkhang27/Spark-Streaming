import requests
import json
import time
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from confluent_kafka import Producer

# --- Configuration ---
BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
KAFKA_TOPIC = "btc-price"
# Fetch frequency: At least once per 100ms = 0.1 seconds
FETCH_INTERVAL_SECONDS = 0.1
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# --- Global flag for graceful shutdown ---
run = True

def shutdown_handler(signum, frame):
    """Handles shutdown signals."""
    global run
    log.info(f"Caught signal {signum}, initiating shutdown...")
    run = False

# --- Kafka Delivery Report Callback ---
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        log.error(f'Message delivery failed: {err}')
    # else:
    #     log.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')


def main():
    """Main function to fetch data and produce to Kafka."""
    global run

    # --- Kafka Producer Setup ---
    try:
        producer_conf = {'bootstrap.servers': KAFKA_BROKERS}
        producer = Producer(producer_conf)
        log.info(f"Kafka producer connected to brokers: {KAFKA_BROKERS}")
    except Exception as e:
        log.fatal(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    # --- Register Signal Handlers ---
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    log.info("Starting price fetch loop...")

    # --- Main Fetch Loop ---
    while run:
        start_time = time.monotonic()

        try:
            # 1. Fetch Price from Binance API
            response = requests.get(BINANCE_API_URL, timeout=5) # Add timeout
            received_timestamp = datetime.now(timezone.utc) # Timestamp right after getting response
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            # 2. Validate and Parse JSON
            try:
                data = response.json()
                if not isinstance(data, dict):
                    raise ValueError("Response is not a JSON object")

                symbol = data.get("symbol")
                price_str = data.get("price")

                if symbol != "BTCUSDT":
                    log.warning(f"Received unexpected symbol: {symbol}. Skipping.")
                    continue # Skip this record

                if price_str is None:
                     log.warning(f"Price field missing in response: {data}. Skipping.")
                     continue

                # Convert price to float
                price_float = float(price_str)

            except (json.JSONDecodeError, ValueError, KeyError) as e:
                log.error(f"Error processing Binance response: {e} - Response: {response.text[:200]}") # Log part of the response
                continue # Skip this record


            # 3. Prepare Kafka Message with ISO8601 Timestamp
            # Ensure milliseconds are included and 'Z' denotes UTC
            timestamp_iso = received_timestamp.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

            message_payload = {
                "symbol": symbol,
                "price": price_float,
                "timestamp": timestamp_iso # Use the formatted ISO string
            }
            message_bytes = json.dumps(message_payload).encode('utf-8')

            # 4. Produce to Kafka (Asynchronously)
            producer.produce(KAFKA_TOPIC,
                             value=message_bytes,
                             key=symbol.encode('utf-8'), # Optional: Use symbol as key for partitioning
                             callback=delivery_report)

            # Trigger delivery report callbacks (non-blocking)
            producer.poll(0)

        except requests.exceptions.Timeout:
            log.warning(f"Request to Binance API timed out.")
        except requests.exceptions.RequestException as e:
            log.error(f"Error fetching price from Binance: {e}")
        except Exception as e:
            log.error(f"An unexpected error occurred: {e}", exc_info=True) # Log traceback for unexpected errors


        # --- Maintain Fetch Interval ---
        elapsed_time = time.monotonic() - start_time
        sleep_duration = max(0, FETCH_INTERVAL_SECONDS - elapsed_time)
        if run: # Avoid sleeping if shutdown signal was received during processing
            time.sleep(sleep_duration)


    # --- Shutdown Sequence ---
    log.info("Exiting fetch loop.")
    log.info("Flushing Kafka producer...")
    try:
        # Wait for all messages in the Producer queue to be delivered.
        remaining_messages = producer.flush(timeout=15) # Wait up to 15 seconds
        if remaining_messages > 0:
             log.warning(f"{remaining_messages} messages were potentially not delivered.")
        else:
             log.info("All messages flushed successfully.")
    except Exception as e:
         log.error(f"Error during producer flush: {e}")

    log.info("Producer closed.")


if __name__ == "__main__":
    main()