# ==========================================
# BLOCK 1: IMPORTS
# ==========================================
import sys
import json
from collections import deque
from datetime import datetime

try:
    import psycopg2
    from confluent_kafka import Consumer, KafkaError
except ImportError as e:
    print(f"❌ BLOCK 1 ERROR: Missing library. Details: {e}")
    sys.exit(1)

# ==========================================
# BLOCK 2: DATABASE CONFIGURATION
# ==========================================
DB_HOST = "timescaledb"  # Docker network name
DB_PORT = "5432"  # Internal Docker port
DB_USER = "postgres"
DB_PASS = "admin"
DB_NAME = "crypto_db"


def setup_database():
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
        conn.autocommit = True
        cursor = conn.cursor()

        # 1. Existing Trades Table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS btc_trades
                       (
                           time
                           TIMESTAMPTZ
                           NOT
                           NULL,
                           price
                           DOUBLE
                           PRECISION,
                           size
                           DOUBLE
                           PRECISION,
                           side
                           VARCHAR
                       (
                           10
                       ),
                           is_whale BOOLEAN
                           );
                       ''')

        # 2. NEW: Volatility Alerts Table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS volatility_alerts
                       (
                           time
                           TIMESTAMPTZ
                           NOT
                           NULL,
                           swing_percentage
                           DOUBLE
                           PRECISION,
                           time_window_seconds
                           INTEGER
                       );
                       ''')

        try:
            cursor.execute("SELECT create_hypertable('btc_trades', by_range('time'), if_not_exists => TRUE);")
            cursor.execute("SELECT create_hypertable('volatility_alerts', by_range('time'), if_not_exists => TRUE);")
        except psycopg2.errors.FeatureNotSupported:
            pass

        print("✅ BLOCK 2 SUCCESS: Connected to DB and verified tables.")
        return conn
    except Exception as e:
        print(f"❌ BLOCK 2 ERROR: Database connection failed. Details: {e}")
        sys.exit(1)


# ==========================================
# BLOCK 3: KAFKA CONSUMER CONFIGURATION
# ==========================================
def setup_kafka_consumer():
    try:
        conf = {
            'bootstrap.servers': 'redpanda:29092',  # Docker network name
            'group.id': 'whale_detector_group',
            'auto.offset.reset': 'latest'
        }
        consumer = Consumer(conf)
        consumer.subscribe(['btc_trades'])
        print("✅ BLOCK 3 SUCCESS: Kafka Consumer subscribed.")
        return consumer
    except Exception as e:
        print(f"❌ BLOCK 3 ERROR: Kafka connection failed. Details: {e}")
        sys.exit(1)


# ==========================================
# BLOCK 4: WHALE & VOLATILITY LOGIC
# ==========================================
WHALE_THRESHOLD_BTC = 0.5

# NEW: Volatility parameters
VOLATILITY_THRESHOLD_PCT = 0.2  # Trigger alert if price swings 0.2% (kept low for testing!)
WINDOW_SECONDS = 60  # Look at the last 60 seconds of data
price_memory = deque()  # A fast list that automatically pushes old data out


def detect_whale(size):
    return size >= WHALE_THRESHOLD_BTC


def detect_volatility(current_time_str, current_price):
    """ Maintains a 60-second sliding window of prices and checks for massive swings. """
    # Convert Coinbase timestamp string to a Python datetime object
    try:
        current_time = datetime.fromisoformat(current_time_str.replace('Z', '+00:00'))
    except Exception:
        return False

    # 1. Add the newest price to our memory
    price_memory.append((current_time, current_price))

    # 2. Kick out any prices that are older than our 60-second window
    while price_memory and (current_time - price_memory[0][0]).total_seconds() > WINDOW_SECONDS:
        price_memory.popleft()

    # 3. We need at least 10 trades in memory to make a fair calculation
    if len(price_memory) < 10:
        return False

    # 4. Find the highest and lowest price in the last 60 seconds
    min_price = min([p[1] for p in price_memory])
    max_price = max([p[1] for p in price_memory])

    # 5. Calculate the percentage swing
    swing_pct = ((max_price - min_price) / min_price) * 100

    # 6. If it's a massive swing, trigger the alert and wipe memory so we don't spam duplicate alerts
    if swing_pct >= VOLATILITY_THRESHOLD_PCT:
        price_memory.clear()
        return swing_pct

    return False


# ==========================================
# BLOCK 5: MAIN INGESTION LOOP
# ==========================================
if __name__ == "__main__":
    print("🚀 BLOCK 5: Starting Consumer with Volatility Detection...")
    db_conn = setup_database()
    cursor = db_conn.cursor()
    kafka_consumer = setup_kafka_consumer()

    try:
        while True:
            msg = kafka_consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            try:
                raw_data = msg.value().decode('utf-8')
                trade = json.loads(raw_data)

                # --- APPLY BUSINESS LOGIC ---
                is_whale = detect_whale(trade['size'])
                swing_pct = detect_volatility(trade['timestamp'], trade['price'])

                # --- SAVE TRADE ---
                cursor.execute(
                    "INSERT INTO btc_trades (time, price, size, side, is_whale) VALUES (%s, %s, %s, %s, %s)",
                    (trade['timestamp'], trade['price'], trade['size'], trade['side'], is_whale)
                )

                # --- SAVE VOLATILITY ALERT (If triggered) ---
                if swing_pct:
                    cursor.execute(
                        "INSERT INTO volatility_alerts (time, swing_percentage, time_window_seconds) VALUES (%s, %s, %s)",
                        (trade['timestamp'], swing_pct, WINDOW_SECONDS)
                    )
                    print(f"📈 VOLATILITY ALERT! Market swung {swing_pct:.2f}% in the last {WINDOW_SECONDS} seconds!")

                # --- PRINT LOGS ---
                if is_whale:
                    print(f"🚨 WHALE DETECTED! {trade['side'].upper()} {trade['size']} BTC @ ${trade['price']:,.2f}")
                else:
                    print(f"💾 Saved standard trade: {trade['size']} BTC")

            except Exception as e:
                print(f"❌ BLOCK 5 ERROR: Failed to process. Details: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        kafka_consumer.close()
        db_conn.close()