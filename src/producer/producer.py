#!/usr/bin/env python3
import json
import logging
from datetime import datetime
import websocket
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKERS = "kafka-0:29092,kafka-1:29093,kafka-2:29094"
KAFKA_TOPIC = "crypto_ticks"
WS_URL = "wss://ws-feed.exchange.coinbase.com"

avro_schema = {
    "type": "record",
    "name": "CryptoTick",
    "fields": [
        {"name": "timestamp", "type": "long"},
        {"name": "product_id", "type": "string"},
        {"name": "price", "type": "double"},
        {"name": "size", "type": "double"},
        {"name": "ask", "type": ["null", "double"]},
        {"name": "bid", "type": ["null", "double"]},
        {"name": "side", "type": ["null", "string"]},
        {"name": "sequence", "type": "long"},
        {"name": "ingestion_timestamp", "type": "long"},
    ]
}

class CoinbaseWebSocket:
    def __init__(self, producer):
        self.producer = producer
        self.ws = None
        
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("type") == "ticker":
                tick = {
                    "timestamp": int(datetime.fromisoformat(data["time"].replace("Z", "+00:00")).timestamp() * 1000),
                    "product_id": data.get("product_id", ""),
                    "price": float(data.get("price", 0)),
                    "size": float(data.get("last_size", 0)),
                    "ask": float(data.get("best_ask", 0)) if data.get("best_ask") else None,
                    "bid": float(data.get("best_bid", 0)) if data.get("best_bid") else None,
                    "side": data.get("side"),
                    "sequence": int(data.get("sequence", 0)),
                    "ingestion_timestamp": int(datetime.now().timestamp() * 1000),
                }
                self.producer.produce(
                    KAFKA_TOPIC,
                    key=data.get("product_id", "").encode(),
                    value=json.dumps(tick).encode()
                )
                logger.info(f"Published: {data['product_id']} @ {data['price']}")
        except Exception as e:
            logger.error(f"Error: {e}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info("WebSocket closed")

    def on_open(self, ws):
        logger.info("WebSocket connected")
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": ["BTC-USD", "ETH-USD"],
            "channels": ["ticker"]
        }
        ws.send(json.dumps(subscribe_msg))

    def start(self):
        self.ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.ws.run_forever()

def main():
    producer_config = {
        'bootstrap.servers': KAFKA_BROKERS,
        'acks': 'all',
        'enable.idempotence': True,
        'compression.type': 'snappy',
    }
    
    producer = Producer(producer_config)
    ws_client = CoinbaseWebSocket(producer)
    
    logger.info("Starting Coinbase WebSocket Producer...")
    ws_client.start()

if __name__ == "__main__":
    main()
