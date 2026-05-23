"""Crypto Data Feeder — streams live BTC/USDT and ETH/USDT ticks from Binance
via ccxt and writes them DIRECTLY to Kafka using confluent-kafka Producer.

No FastAPI dependency. Low-latency optimized:
  - linger.ms=5       : flush every 5ms (vs default 5ms, tuned for low latency)
  - compression=none   : skip CPU compression overhead
  - acks=1            : wait for leader only (not full ISR)
"""
import json
import logging
import sys
import time
from datetime import datetime, timezone

import ccxt
from confluent_kafka import Producer

# ── Kafka Tuning ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "crypto_ticks"

# Low-latency producer config
PRODUCER_CONF = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "linger.ms": "5",          # Flush rapidly every 5ms
    "compression.type": "none", # No compression — saves CPU cycles
    "acks": "1",               # Leader only — fastest acknowledgement
    "queue.buffering.max.messages": "100000",
    "queue.buffering.max.kbytes": "1048576",
    "batch.num.messages": "100",
    "message.timeout.ms": "10000",
}

SYMBOLS = ["BTC/USDT", "ETH/USDT"]
FETCH_INTERVAL = 2  # seconds between ticks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("crypto-feeder")


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)
    else:
        logger.debug("Delivered to %s [%d] offset %d",
                     msg.topic(), msg.partition(), msg.offset())


def _publish_to_kafka(producer: Producer, symbol: str, price: float,
                      volume: float, timestamp: datetime, exchange: str) -> None:
    payload = {
        "symbol": symbol,
        "price": str(price),
        "volume": str(volume),
        "timestamp": timestamp.isoformat(),
        "exchange": exchange,
    }
    try:
        producer.produce(
            KAFKA_TOPIC,
            key=symbol.encode("utf-8"),
            value=json.dumps(payload, default=str).encode("utf-8"),
            callback=_delivery_report,
        )
        # Poll(0) triggers immediate flush without blocking
        producer.poll(0)
    except Exception as exc:
        logger.error("Kafka produce error for %s: %s", symbol, exc)


def fetch_and_publish(exchange: ccxt.Exchange, producer: Producer, symbol: str) -> None:
    try:
        ohlcv = exchange.fetch_ticker(symbol)
        ts = datetime.fromtimestamp(ohlcv["timestamp"] / 1000, tz=timezone.utc)
        _publish_to_kafka(
            producer,
            symbol=symbol,
            price=ohlcv["last"],
            volume=ohlcv["baseVolume"],
            timestamp=ts,
            exchange="binance",
        )
        logger.info("→ Kafka %s  price=%.4f  volume=%.6f", symbol, ohlcv["last"], ohlcv["baseVolume"])
    except Exception as exc:
        logger.error("Error fetching %s: %s", symbol, exc)


def main() -> None:
    logger.info("Starting Crypto Kafka Feeder (direct — no FastAPI)")
    logger.info("Kafka: %s  Topic: %s  Symbols: %s  Interval: %ds",
                KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SYMBOLS, FETCH_INTERVAL)

    producer = Producer(PRODUCER_CONF)
    logger.info("Kafka Producer config: linger=5ms compression=none acks=1")

    exchange = ccxt.binance({"enableRateLimit": True})
    logger.info("ccxt exchange: %s (rate limit: %s)", exchange.id, exchange.rateLimit)

    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka broker at %s ...", KAFKA_BOOTSTRAP_SERVERS)
    for attempt in range(1, 31):
        try:
            metadata = producer.list_topics(timeout=10)
            if KAFKA_TOPIC in metadata.topics:
                logger.info("Topic '%s' found in broker metadata", KAFKA_TOPIC)
            else:
                logger.warning("Topic '%s' not yet visible (attempt %d)", KAFKA_TOPIC, attempt)
            break
        except Exception as exc:
            logger.warning("Attempt %d/30 — Kafka not ready: %s", attempt, exc)
            time.sleep(2)
    else:
        logger.error("Kafka broker not available after 30 attempts — exiting")
        sys.exit(1)

    logger.info("Entering fetch loop …")
    while True:
        for symbol in SYMBOLS:
            fetch_and_publish(exchange, producer, symbol)
            time.sleep(0.5)  # stagger to avoid burst
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()
