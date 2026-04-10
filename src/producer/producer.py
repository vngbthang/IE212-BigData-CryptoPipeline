import json
import os
import random
import signal
import sys
import time
import traceback
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = "kafka-0:9092,kafka-1:9092,kafka-2:9092"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_ticks")

INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "1"))
MAX_CONNECT_RETRIES = 10
RETRY_DELAY_SECONDS = 5

PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD"]
TICK_TYPE = "ticker"

RUNNING = True


def handle_shutdown(signum, frame):
    del signum, frame
    global RUNNING
    RUNNING = False


def generate_tick() -> dict:
    product_id = random.choice(PRODUCTS)
    base_price = {
        "BTC-USD": random.uniform(60000, 80000),
        "ETH-USD": random.uniform(2500, 5000),
        "SOL-USD": random.uniform(80, 250),
    }[product_id]
    size = random.uniform(0.001, 2.0)

    return {
        "type": TICK_TYPE,
        "product_id": product_id,
        "price": f"{base_price:.2f}",
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "size": f"{size:.6f}",
    }


def create_producer() -> KafkaProducer:
    brokers = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]

    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(3, 3, 1),
                acks=1,
                retries=5,
                linger_ms=0,
            )
            print(
                f"[MOCK] Connected to Kafka on attempt {attempt}/{MAX_CONNECT_RETRIES}",
                flush=True,
            )
            return producer
        except NoBrokersAvailable:
            print("Đang đợi Kafka...", flush=True)
            if attempt == MAX_CONNECT_RETRIES:
                print("[MOCK][ERROR] Exhausted Kafka connection retries", flush=True)
                raise
            time.sleep(RETRY_DELAY_SECONDS)
        except Exception as exc:
            print(f"[MOCK][ERROR] Failed to create Kafka producer: {exc}", flush=True)
            print(traceback.format_exc(), flush=True)
            raise

    raise RuntimeError("Unable to create KafkaProducer after retries")


def main() -> int:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    producer = create_producer()
    print(
        f"[MOCK] Producer started -> topic={KAFKA_TOPIC}, brokers={KAFKA_BOOTSTRAP_SERVERS}",
        flush=True,
    )

    try:
        while RUNNING:
            payload = generate_tick()
            future = producer.send(KAFKA_TOPIC, payload)
            metadata = future.get(timeout=10)
            producer.flush()
            print(
                f"[MOCK] Sent -> topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}, payload={payload}",
                flush=True,
            )
            time.sleep(INTERVAL_SECONDS)
    except Exception as exc:
        print(f"[MOCK][ERROR] {exc}", flush=True)
        print(traceback.format_exc(), flush=True)
        return 1
    finally:
        producer.close()
        print("[MOCK] Producer stopped", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
