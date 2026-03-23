# ==========================================
# BLOCK 1: IMPORTS
# ==========================================
import sys
import ssl  # <--- NEW: Added ssl library
try:
    import websocket
    import json
    from confluent_kafka import Producer
except ImportError as e:
    print(f"❌ BLOCK 1 ERROR: Missing library. Did you run 'pip install confluent-kafka websocket-client'? Details: {e}")
    sys.exit(1)

# ==========================================
# BLOCK 2: KAFKA CONFIGURATION
# ==========================================
try:
    conf = {'bootstrap.servers': 'redpanda:29092'}
    producer = Producer(conf)
    KAFKA_TOPIC = 'btc_trades'
    print("✅ BLOCK 2 SUCCESS: Kafka Producer initialized.")
except Exception as e:
    print(f"❌ BLOCK 2 ERROR: Failed to connect to Kafka. Is Docker running? Details: {e}")
    sys.exit(1)


# ==========================================
# BLOCK 3: KAFKA DELIVERY CALLBACK
# ==========================================
def delivery_report(err, msg):
    """ Triggered when a message successfully hits Kafka, or fails. """
    if err is not None:
        print(f"❌ BLOCK 3 ERROR: Kafka rejected the message: {err}")
    else:
        # NOTE: Comment out the print line below if your terminal gets too messy!
        print(f"✅ BLOCK 3 SUCCESS: Trade pushed to Kafka -> {msg.value().decode('utf-8')}")


# ==========================================
# BLOCK 4: DATA PROCESSING
# ==========================================
def process_trade_data(raw_message):
    """ Extracts only the exact fields we need from the Coinbase JSON firehose. """
    try:
        data = json.loads(raw_message)

        # We only want 'match' events (actual trades)
        if data.get('type') == 'match':
            trade_data = {
                'timestamp': data.get('time'),
                'price': float(data.get('price')),
                'size': float(data.get('size')),
                'side': data.get('side')  # 'buy' or 'sell'
            }
            return json.dumps(trade_data)
        return None
    except Exception as e:
        print(f"❌ BLOCK 4 ERROR: Failed to parse trade data. Details: {e}")
        return None


# ==========================================
# BLOCK 5: WEBSOCKET EVENT HANDLERS
# ==========================================
def on_message(ws, message):
    processed_json = process_trade_data(message)

    if processed_json:
        try:
            # Send the cleaned data to Kafka
            producer.produce(KAFKA_TOPIC, value=processed_json, callback=delivery_report)
            producer.poll(0)
        except Exception as e:
            print(f"❌ BLOCK 5 ERROR: Failed to push to Kafka topic. Details: {e}")


def on_open(ws):
    try:
        print("🔌 BLOCK 5: Connected to Coinbase WebSocket!")
        subscribe_message = {
            "type": "subscribe",
            "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}]
        }
        ws.send(json.dumps(subscribe_message))
        print(f"📡 BLOCK 5: Listening for BTC-USD trades...")
    except Exception as e:
        print(f"❌ BLOCK 5 ERROR: Failed to send subscription message. Details: {e}")


def on_error(ws, error):
    print(f"❌ BLOCK 5 ERROR: WebSocket connection error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("🔴 BLOCK 5: WebSocket Closed.")


# ==========================================
# BLOCK 6: MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    try:
        print("🚀 BLOCK 6: Starting Producer Application...")
        ws = websocket.WebSocketApp("wss://ws-feed.exchange.coinbase.com",
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)

        # NEW: We tell the websocket to bypass strict SSL verification
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    except KeyboardInterrupt:
        print("\n🛑 BLOCK 6: Manually stopped by user.")
    except Exception as e:
        print(f"❌ BLOCK 6 ERROR: Fatal crash. Details: {e}")