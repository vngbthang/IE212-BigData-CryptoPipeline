#!/usr/bin/env python3
# ============================================================================
# PYTHON PRODUCER - BRONZE LAYER INGESTION
# Real-time Crypto Tick Data from Coinbase WebSocket → Kafka (Avro)
# ============================================================================

import json
import logging
import time
import io
import os
from typing import Dict, Optional, Any
from datetime import datetime

import websocket
import avro.schema
import avro.io
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import threading

# ============================================================================
# CONFIGURATION
# ============================================================================
# Kafka Configuration
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka-0:29092,kafka-1:29093,kafka-2:29094')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_ticks')
KAFKA_PARTITION_KEY_PREFIX = 'crypto'

# Coinbase WebSocket Configuration
COINBASE_WS_URL = 'wss://ws-feed.exchange.coinbase.com'
PRODUCTS = ['BTC-USD', 'ETH-USD']  # Products to subscribe

# Retry Configuration
MAX_RETRIES = 10
RETRY_BACKOFF_BASE = 1  # seconds
RETRY_BACKOFF_MAX = 30  # seconds

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# ============================================================================
# LOGGING SETUP
# ============================================================================
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL))

handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# ============================================================================
# AVRO SCHEMA (Matches Bronze Layer)
# ============================================================================
AVRO_SCHEMA_STR = '''
{
  "namespace": "com.cryptopipeline.bronze",
  "type": "record",
  "name": "CryptoTickEvent",
  "fields": [
    {
      "name": "timestamp",
      "type": "long",
      "doc": "Unix timestamp in milliseconds when price was published"
    },
    {
      "name": "product_id",
      "type": "string",
      "doc": "Product identifier (e.g., BTC-USD)"
    },
    {
      "name": "exchange",
      "type": "string",
      "default": "coinbase",
      "doc": "Exchange name"
    },
    {
      "name": "price",
      "type": "double",
      "doc": "Current price"
    },
    {
      "name": "size",
      "type": "double",
      "doc": "Amount of crypto traded"
    },
    {
      "name": "bid",
      "type": ["null", "double"],
      "default": null,
      "doc": "Current best bid price"
    },
    {
      "name": "ask",
      "type": ["null", "double"],
      "default": null,
      "doc": "Current best ask price"
    },
    {
      "name": "side",
      "type": {
        "type": "enum",
        "name": "TradeSide",
        "symbols": ["buy", "sell", "unknown"]
      },
      "default": "unknown",
      "doc": "Trade side (buy/sell/unknown)"
    },
    {
      "name": "sequence",
      "type": "long",
      "doc": "Order book sequence number"
    },
    {
      "name": "ingestion_timestamp",
      "type": "long",
      "doc": "Unix timestamp in ms when data was ingested into Kafka"
    }
  ]
}
'''

# ============================================================================
# AVRO SCHEMA LOADER
# ============================================================================
def load_avro_schema() -> avro.schema.SchemaFromJSONData:
    """
    Load and validate Avro schema.
    
    Returns:
        Parsed Avro schema object
    """
    try:
        schema = avro.schema.parse(AVRO_SCHEMA_STR)
        logger.info(f"✓ Avro schema loaded successfully: {schema.name}")
        return schema
    except Exception as e:
        logger.error(f"✗ Failed to parse Avro schema: {e}")
        raise

# ============================================================================
# AVRO SERIALIZATION
# ============================================================================
def serialize_to_avro(data: Dict[str, Any], schema: avro.schema.SchemaFromJSONData) -> bytes:
    """
    Serialize data to Avro binary format.
    
    Args:
        data: Dictionary containing tick data
        schema: Parsed Avro schema
        
    Returns:
        Binary Avro-encoded data
        
    Raises:
        Exception: If serialization fails
    """
    try:
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer = avro.io.DatumWriter(schema)
        writer.write(data, encoder)
        return bytes_writer.getvalue()
    except Exception as e:
        logger.error(f"✗ Avro serialization error: {e}")
        logger.error(f"  Data: {data}")
        raise

# ============================================================================
# KAFKA PRODUCER CALLBACK
# ============================================================================
def kafka_delivery_report(err: Optional[KafkaError], msg) -> None:
    """
    Callback for Kafka message delivery confirmation.
    
    Args:
        err: Error object (None = success, KafkaError = failure)
        msg: Kafka message object
    """
    if err is not None:
        logger.warning(f"Message delivery failed: {err}")
        # Could implement dead-letter queue here
    else:
        logger.debug(
            f"✓ Message produced successfully to {msg.topic()} "
            f"[partition={msg.partition()}, offset={msg.offset()}]"
        )

# ============================================================================
# KAFKA PRODUCER SETUP
# ============================================================================
def create_kafka_producer() -> Producer:
    """
    Create Kafka producer with optimized settings.
    
    Returns:
        Configured Kafka Producer client
        
    Raises:
        Exception: If producer creation fails
    """
    producer_config = {
        'bootstrap.servers': KAFKA_BROKERS,
        'client.id': 'crypto-producer-001',
        
        # Reliability settings
        'acks': 'all',  # Wait for all in-sync replicas
        'retries': 10,
        'max.in.flight.requests.per.connection': 5,
        
        # Performance settings (tuned for throughput)
        'compression.type': 'snappy',
        'batch.size': 32000,  # 32KB batches
        'linger.ms': 10,  # Wait max 10ms to batch
        'buffer.memory': 67108864,  # 64MB buffer
        
        # Network settings
        'socket.keepalive.enable': True,
        'connections.max.idle.ms': 540000,  # 9 minutes
        
        # Timeout settings
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 120000,
    }
    
    try:
        producer = Producer(producer_config)
        logger.info("✓ Kafka producer created successfully")
        logger.info(f"  Brokers: {KAFKA_BROKERS}")
        logger.info(f"  Topic: {KAFKA_TOPIC}")
        return producer
    except Exception as e:
        logger.error(f"✗ Failed to create Kafka producer: {e}")
        raise

# ============================================================================
# COINBASE WEBSOCKET CLIENT
# ============================================================================
class CoinbaseWebSocketClient:
    """
    WebSocket client for Coinbase tick data feed.
    Handles auto-reconnection with exponential backoff.
    """
    
    def __init__(self, schema: avro.schema.SchemaFromJSONData, producer: Producer):
        """
        Initialize WebSocket client.
        
        Args:
            schema: Avro schema for serialization
            producer: Kafka producer instance
        """
        self.schema = schema
        self.producer = producer
        self.ws = None
        self.retry_count = 0
        self.should_run = True
        
        # Statistics
        self.messages_sent = 0
        self.errors_occurred = 0
        self.reconnects = 0
        
    def on_open(self, ws) -> None:
        """Callback when WebSocket connection opens."""
        logger.info("✓ WebSocket connection opened")
        self.retry_count = 0  # Reset retry counter on success
        self.send_subscribe_message()
        
    def on_close(self, ws, close_status_code, close_msg) -> None:
        """Callback when WebSocket connection closes."""
        logger.warning(f"✗ WebSocket connection closed: {close_msg}")
        
        if self.should_run:  # Only reconnect if we didn't intentionally close
            self.attempt_reconnect()
        
    def on_error(self, ws, error) -> None:
        """Callback when WebSocket error occurs."""
        logger.error(f"✗ WebSocket error: {error}")
        self.errors_occurred += 1
        
        if self.should_run and self.errors_occurred > MAX_RETRIES:
            logger.critical(f"✗ Max error threshold ({MAX_RETRIES}) reached. Stopping.")
            self.should_run = False
            
    def on_message(self, ws, message: str) -> None:
        """
        Callback when WebSocket message received.
        
        Args:
            ws: WebSocket connection
            message: JSON message from Coinbase
        """
        try:
            data = json.loads(message)
            
            # Filter: only process 'ticker' type messages (price ticks)
            if data.get('type') != 'ticker':
                return
            
            # Map Coinbase JSON to Avro record
            time_obj = datetime.fromisoformat(data['time'].replace('Z', '+00:00'))
            tick_timestamp = int(time_obj.timestamp() * 1000)  # milliseconds
            
            tick_record = {
                'timestamp': tick_timestamp,
                'product_id': data['product_id'],
                'exchange': 'coinbase',
                'price': float(data.get('price', 0)),
                'size': float(data.get('last_size', 0)),
                'bid': float(data.get('best_bid')) if data.get('best_bid') else None,
                'ask': float(data.get('best_ask')) if data.get('best_ask') else None,
                'side': data.get('side', 'unknown').lower(),
                'sequence': int(data.get('sequence', 0)),
                'ingestion_timestamp': int(time.time() * 1000),
            }
            
            # Serialize to Avro
            avro_bytes = serialize_to_avro(tick_record, self.schema)
            
            # Produce to Kafka (async, with callback)
            partition_key = f"{KAFKA_PARTITION_KEY_PREFIX}:{tick_record['product_id']}".encode()
            self.producer.produce(
                topic=KAFKA_TOPIC,
                key=partition_key,
                value=avro_bytes,
                on_delivery=kafka_delivery_report,
            )
            
            self.messages_sent += 1
            
            # Log statistics every 100 messages
            if self.messages_sent % 100 == 0:
                logger.info(
                    f"📊 Progress: {self.messages_sent} messages sent | "
                    f"Errors: {self.errors_occurred} | "
                    f"Reconnects: {self.reconnects}"
                )
            
        except json.JSONDecodeError as e:
            logger.warning(f"✗ Failed to parse message as JSON: {e}")
            self.errors_occurred += 1
        except Exception as e:
            logger.error(f"✗ Error processing message: {e}")
            logger.error(f"  Message: {message[:100]}...")  # Log first 100 chars
            self.errors_occurred += 1
            
    def send_subscribe_message(self) -> None:
        """Send subscription message to Coinbase WebSocket."""
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": PRODUCTS,
            "channels": ["ticker"]
        }
        
        try:
            self.ws.send(json.dumps(subscribe_msg))
            logger.info(f"✓ Subscribed to: {PRODUCTS}")
        except Exception as e:
            logger.error(f"✗ Failed to send subscribe message: {e}")
            raise
            
    def attempt_reconnect(self) -> None:
        """Attempt to reconnect with exponential backoff."""
        if self.retry_count >= MAX_RETRIES:
            logger.critical(f"✗ Max retries ({MAX_RETRIES}) reached. Giving up.")
            self.should_run = False
            return
            
        backoff = min(
            RETRY_BACKOFF_BASE * (2 ** self.retry_count),
            RETRY_BACKOFF_MAX
        )
        self.retry_count += 1
        self.reconnects += 1
        
        logger.info(f"⏳ Attempting reconnection #{self.retry_count} in {backoff}s...")
        time.sleep(backoff)
        self.connect()
        
    def connect(self) -> None:
        """Establish WebSocket connection."""
        try:
            logger.info(f"🔗 Connecting to: {COINBASE_WS_URL}")
            self.ws = websocket.WebSocketApp(
                COINBASE_WS_URL,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
        except Exception as e:
            logger.error(f"✗ Failed to create WebSocket client: {e}")
            raise
            
    def run(self) -> None:
        """Start WebSocket connection (blocking)."""
        self.connect()
        
        # Run with reconnect enabled
        while self.should_run:
            try:
                self.ws.run_forever(
                    ping_interval=30,  # Send ping every 30s to keep connection alive
                    ping_timeout=10,
                )
            except Exception as e:
                logger.error(f"✗ WebSocket run error: {e}")
                if self.should_run:
                    self.attempt_reconnect()
                    
    def shutdown(self) -> None:
        """Gracefully shutdown the client."""
        logger.info("🛑 Shutting down WebSocket client...")
        self.should_run = False
        
        if self.ws:
            self.ws.close()
            
        # Flush remaining messages from Kafka producer
        self.producer.flush(timeout=10)
        logger.info(f"✓ Shutdown complete. {self.messages_sent} messages processed.")

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
def main():
    """
    Main entry point for the ingestion producer.
    """
    logger.info("=" * 80)
    logger.info("🚀 REAL-TIME CRYPTO DATA PIPELINE - INGESTION PRODUCER")
    logger.info("=" * 80)
    logger.info(f"Configuration:")
    logger.info(f"  Kafka Brokers: {KAFKA_BROKERS}")
    logger.info(f"  Topic: {KAFKA_TOPIC}")
    logger.info(f"  Products: {PRODUCTS}")
    logger.info(f"  WebSocket URL: {COINBASE_WS_URL}")
    logger.info("=" * 80)
    
    try:
        # Load schema
        schema = load_avro_schema()
        
        # Create Kafka producer
        producer = create_kafka_producer()
        
        # Create and run WebSocket client
        ws_client = CoinbaseWebSocketClient(schema, producer)
        
        # Handle graceful shutdown
        def signal_handler(signum, frame):
            logger.info("📢 Received shutdown signal")
            ws_client.shutdown()
            
        import signal
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start WebSocket connection
        ws_client.run()
        
    except KeyboardInterrupt:
        logger.info("✓ Interrupted by user")
    except Exception as e:
        logger.error(f"✗ Fatal error: {e}", exc_info=True)
        return 1
        
    return 0

if __name__ == '__main__':
    exit(main())
