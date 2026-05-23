"""FastAPI service for ingesting cryptocurrency tick data.

Publishes crypto tick payloads to Kafka topic `crypto_ticks`.
"""
import io
import json
import logging
from datetime import datetime
from typing import Annotated

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from confluent_kafka import Producer
import minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Crypto Tick Ingestion API", version="1.0.0")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "crypto_ticks"
MINIO_ENDPOINT = "storage:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET = "crypto-ticks"


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic schemas
# ─────────────────────────────────────────────────────────────────────────────

class CryptoTick(BaseModel):
    symbol: str = Field(..., examples=["BTC/USDT", "ETH/USDT"], description="Trading pair symbol")
    price: float = Field(..., gt=0, description="Last traded price")
    volume: float = Field(..., ge=0, description="Trade volume in quote currency")
    timestamp: datetime = Field(..., description="UTC timestamp of the tick")
    exchange: str = Field(default="binance", description="Exchange source")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "symbol": "BTC/USDT",
                    "price": 67543.21,
                    "volume": 0.5832,
                    "timestamp": "2026-05-21T17:00:00Z",
                    "exchange": "binance",
                }
            ]
        }
    }


class BatchTicks(BaseModel):
    ticks: list[CryptoTick]


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer
# ─────────────────────────────────────────────────────────────────────────────

_producer: Producer | None = None
_mc_client: minio.Minio | None = None


def _get_mc() -> minio.Minio:
    global _mc_client
    if _mc_client is None:
        _mc_client = minio.Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
    return _mc_client


def _ensure_bucket() -> None:
    mc = _get_mc()
    if not mc.bucket_exists(MINIO_BUCKET):
        mc.make_bucket(MINIO_BUCKET)
        logger.info("Created MinIO bucket: %s", MINIO_BUCKET)


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    return _producer


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)
    else:
        logger.debug("Delivered to %s [%d]", msg.topic(), msg.partition())


def _publish_to_kafka(payload: dict) -> None:
    prod = _get_producer()
    prod.produce(
        KAFKA_TOPIC,
        key=payload["symbol"].encode("utf-8"),
        value=json.dumps(payload, default=str).encode("utf-8"),
        callback=_delivery_report,
    )
    prod.poll(0)


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan startup
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
def on_startup():
    try:
        _ensure_bucket()
    except Exception as exc:
        logger.warning("MinIO bucket init skipped (will retry on first upload): %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload_tick(
    symbol: Annotated[str, Form(description="Trading pair, e.g. BTC/USDT")],
    price: Annotated[float, Form(description="Last traded price")],
    volume: Annotated[float, Form(description="Trade volume")],
    timestamp: Annotated[str, Form(description="UTC ISO 8601 timestamp")],
    exchange: Annotated[str, Form(description="Exchange name")] = "binance",
):
    """Upload a single crypto tick as form fields.

    Example:
        curl -X POST http://localhost:8000/upload/BTC_USDT \
             -F "price=67543.21" \
             -F "volume=0.5832" \
             -F "timestamp=2026-05-21T17:00:00Z" \
             -F "exchange=binance"
    """
    try:
        ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp format: {e}")

    tick = CryptoTick(symbol=symbol, price=price, volume=volume, timestamp=ts, exchange=exchange)
    payload = tick.model_dump(mode="json")

    _publish_to_kafka(payload)

    logger.info("Published tick: %s @ %.4f", payload["symbol"], payload["price"])
    return {"status": "accepted", "symbol": payload["symbol"], "price": payload["price"]}


@app.post("/upload_batch")
async def upload_batch(ticks: BatchTicks):
    """Upload multiple crypto ticks as a JSON body.

    Example:
        curl -X POST http://localhost:8000/upload_batch \\
             -H "Content-Type: application/json" \\
             -d '{"ticks": [{"symbol": "BTC/USDT", "price": 67543.21, ...}]}'
    """
    published = []
    for tick in ticks.ticks:
        payload = tick.model_dump(mode="json")
        _publish_to_kafka(payload)
        published.append(payload["symbol"])

    logger.info("Published %d ticks: %s", len(published), published)
    return {"status": "accepted", "count": len(published)}


@app.post("/upload_file")
async def upload_file(file: UploadFile = File(...)):
    """Upload a raw JSON file containing a list of crypto ticks.

    The file should contain a JSON array of tick objects.
    Each tick must have: symbol, price, volume, timestamp, exchange (optional).
    """
    if not file.filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted")

    content = await file.read()

    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    if isinstance(data, dict) and "ticks" in data:
        tick_list = data["ticks"]
    elif isinstance(data, list):
        tick_list = data
    else:
        raise HTTPException(
            status_code=400,
            detail="JSON must be a list of ticks or an object with a 'ticks' key",
        )

    for raw in tick_list:
        tick = CryptoTick(**raw)
        payload = tick.model_dump(mode="json")
        _publish_to_kafka(payload)

    logger.info("Uploaded file '%s' with %d ticks", file.filename, len(tick_list))
    return {"status": "accepted", "count": len(tick_list)}
