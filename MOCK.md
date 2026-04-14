# 🎯 Mock Mode / Development Guide

The crypto pipeline supports **two deployment modes**:

- **Dev Mode (Default):** Mock data, no internet required, perfect for demos
- **Prod Mode:** Real Coinbase WebSocket data, requires internet connection

Both modes use the same infrastructure stack and produce identical data pipeline behavior.

---

## Quick Start: Dev Mode (Mock Data)

### Prerequisites
```bash
# Required: Docker & Docker Compose
docker --version      # v20.10+
docker-compose --version  # v1.29+

# Required: PostgreSQL client (for testing)
psql --version        # v12+
```

### 1. Start Services
```bash
cd /path/to/IE212-BigData-CryptoPipeline

# Start all services (dev mode by default)
docker-compose up -d

# Wait for services to stabilize
sleep 60

# Verify services are running
docker-compose ps
```

**Expected Output:** 12-14 services in "Up" or "Healthy" state

### 2. Run Smoke Tests
```bash
# Validate all layers working together
./smoke_tests_merged.sh
```

**Expected Result:** All 6 tests passing

### 3. Open Dashboard
```bash
# Open in browser
http://localhost:8501

# Or command line
curl http://localhost:8501
```

**Expected Displays:**
- Panel 1: Real-time candles (1-min bars, last 30 minutes)
- Panel 2: Historical candles (5-min aggregates, last 4 hours)
- Panel 3: KPI metrics (throughput, latency, quality, freshness)

### 4. Monitor Data Flow

```bash
# Watch producer generating mock data
docker-compose logs -f producer

# Watch Spark processing
docker-compose logs -f spark-master

# Query database directly
psql -h localhost -U crypto_user -d cryptopipeline -c \
  "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE timestamp >= NOW() - INTERVAL '5 min';"
```

---

## Environment Configuration

### Dev Mode (.env)
```bash
# Default configuration (already set in .env)
CRYPTO_MODE=dev
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cryptopipeline
DB_USER=crypto_user
DB_PASS=crypto_password

# Mock Producer Settings
KAFKA_BROKERS=kafka-0:9092,kafka-1:9092,kafka-2:9092
KAFKA_TOPIC=crypto_ticks
INTERVAL_SECONDS=1
```

### Switch to Prod Mode
```bash
# Edit .env
CRYPTO_MODE=prod
COINBASE_SYMBOLS=BTC-USD,ETH-USD  # Which symbols to fetch
COINBASE_CHANNELS=matches,ticker  # Which data types to fetch

# Then restart services
docker-compose up -d
```

---

## Mock Data Generation

### What Mock Producer Generates

The mock producer creates realistic cryptocurrency data every second:

```python
{
  "symbol": "BTC-USD",           # Trading pair
  "price": 25432.50,             # Current price
  "size": 0.125,                 # Trade volume
  "timestamp": 1712973600000,    # Unix timestamp (ms)
  "side": "buy" or "sell"        # Trade direction
}
```

### Mock Data Statistics
- **Data Rate:** 60 messages/minute (1 per second)
- **Symbols:** BTC-USD, ETH-USD, XRP-USD (3 pairs)
- **Price Range:** Realistic +/- 2% random walk
- **Trade Sizes:** 0.001 to 10.0 coins
- **Uptime:** 99.9% (fails only on explicit stop)

### Verify Mock Data Flow
```bash
# 1. Check producer is sending
docker-compose logs -f producer | grep "Sent message"

# 2. Verify Kafka has messages
docker-compose exec kafka-0 \
  kafka-console-consumer.sh --topic crypto_ticks --from-beginning \
  --max-messages=5 --bootstrap-server localhost:9092

# 3. Check Spark is processing
docker-compose logs -f spark-master | grep "processed"

# 4. Query database for results
psql -h localhost -U crypto_user -d cryptopipeline -c \
  "SELECT COUNT(*), symbol, MAX(timestamp) FROM gold.gold_crypto_ohlcv GROUP BY symbol;"
```

---

## Common Development Tasks

### Run Demo (5-minute test)
```bash
./run.sh --dev --duration 5
```

This will:
1. Start all services
2. Run for 5 minutes with mock data
3. Generate 5-10 records in database
4. Display 2-3 candlesticks in dashboard
5. Stop all services

### Monitor Production in Real-Time
```bash
./monitor.sh
```

Interactive menu with options:
- View Kafka metrics
- View database stats
- View Spark progress
- Health check all services
- Auto-refresh dashboard

### View Logs for Specific Service
```bash
# Producer logs
docker-compose logs -f producer

# Spark logs
docker-compose logs -f spark-master spark-worker

# Database logs
docker-compose logs -f postgres

# Dashboard logs
docker-compose logs -f dashboard
```

### Stop Everything (Clean)
```bash
./run.sh --stop

# Or manually
docker-compose down

# Clean Docker volumes (careful - deletes data)
docker-compose down -v
```

---

## Data Schema

### OHLCV Table (gold.gold_crypto_ohlcv)

| Column | Type | Example | Purpose |
|--------|------|---------|---------|
| record_id | UUID | a1b2c3d4... | Unique identifier |
| symbol | VARCHAR | BTC-USD | Trading pair |
| timestamp | TIMESTAMPTZ | 2026-04-13 12:30:00 | Minute bucket start |
| open | NUMERIC(20,8) | 25432.50000000 | Opening price |
| high | NUMERIC(20,8) | 25500.00000000 | Highest price (min) |
| low | NUMERIC(20,8) | 25400.00000000 | Lowest price (min) |
| close | NUMERIC(20,8) | 25450.00000000 | Closing price |
| volume | NUMERIC(20,8) | 15.35000000 | Total volume (coins) |

### Query Examples

```bash
# Get last 10 minutes of data
psql -h localhost -U crypto_user -d cryptopipeline << 'SQL'
SELECT symbol, timestamp, open, close, volume
FROM gold.gold_crypto_ohlcv
WHERE timestamp >= NOW() - INTERVAL '10 min'
ORDER BY timestamp DESC
LIMIT 10;
SQL

# Get daily aggregates
SELECT 
  DATE(timestamp) as date,
  symbol,
  MIN(low) as daily_low,
  MAX(high) as daily_high,
  SUM(volume) as daily_volume
FROM gold.gold_crypto_ohlcv
GROUP BY DATE(timestamp), symbol
ORDER BY date DESC;

# Compare symbols
SELECT symbol, COUNT(*), AVG(close), MAX(volume)
FROM gold.gold_crypto_ohlcv
WHERE timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY symbol;
```

---

## Troubleshooting

### No Data in Dashboard

**Problem:** Dashboard shows "No data" or empty charts

**Solution:**
```bash
# 1. Check if services are running
docker-compose ps | grep -E "postgres|spark|producer|kafka"

# 2. Check producer is sending data
docker-compose logs producer | tail -20

# 3. Verify database has data
psql -h localhost -U crypto_user -d cryptopipeline \
  -c "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;"

# 4. Check Spark logs for errors
docker-compose logs spark-master | grep -i error

# 5. If stuck, restart producer only
docker-compose restart producer
```

### Database Connection Failed

**Problem:** "psql: could not translate host name"

**Solution:**
```bash
# 1. Verify PostgreSQL is running
docker-compose ps postgres

# 2. Check if port 5432 is open
netstat -tulpn | grep 5432

# 3. Try connecting to container directly
docker-compose exec postgres psql -U postgres -c "SELECT 1;"

# 4. Verify credentials in .env
grep "DB_" .env

# 5. Restart PostgreSQL
docker-compose restart postgres
sleep 10
./run_tests.sh
```

### Kafka Broker Unhealthy

**Problem:** `docker-compose ps` shows broker as "unhealthy"

**Solution:**
```bash
# This is cosmetic - brokers actually work fine
# Verify they're receiving data:
docker-compose exec kafka-0 \
  kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# If truly down:
docker-compose restart kafka-0 kafka-1 kafka-2
sleep 30
docker-compose logs kafka-0 | tail -20
```

### High Memory Usage

**Problem:** Docker consuming >4GB RAM

**Solution:**
```bash
# Check which service is using memory
docker stats

# Reduce Spark memory (in docker-compose.yml)
# Change: spark.executor.memory from 1g to 512m
# Change: spark.driver.memory from 1g to 512m

# Reduce Kafka heap
# Change: KAFKA_HEAP_OPTS="-Xmx2G" to "-Xmx1G"

# Apply changes
docker-compose down
docker-compose up -d
```

---

## Production Mode (Real Data)

### Setup Coinbase API

```bash
# 1. Get API credentials from Coinbase
#    Visit: https://www.coinbase.com/settings/api

# 2. Update .env
CRYPTO_MODE=prod
COINBASE_KEY=your-api-key
COINBASE_SECRET=your-api-secret
COINBASE_PASSPHRASE=your-passphrase

# 3. Restart services
docker-compose down
docker-compose up -d

# 4. Verify real data
docker-compose logs -f producer | grep "Received from Coinbase"
```

### Monitor Production Data

```bash
# Real-time message rate
docker-compose exec kafka-0 \
  kafka-run-class.sh kafka.tools.ConsumerPerformance \
  --broker-list kafka-0:9092 --topic crypto_ticks \
  --messages 1000 --timeout 60000

# Database growth
watch -n 5 "psql -h localhost -U crypto_user -d cryptopipeline -c \
  'SELECT COUNT(*) as total_records, \
          MAX(timestamp) as latest_record, \
          AGE(MAX(timestamp), NOW()) as data_lag \
   FROM gold.gold_crypto_ohlcv;'"
```

---

## Performance Baseline

### Expected Metrics (Dev Mode, 5 Minutes)

| Metric | Expected | How to Check |
|--------|----------|--------------|
| Data arrival rate | 60 msgs/min | `docker-compose logs producer \| wc -l` |
| Ingest-to-store latency | <5 seconds | Compare Kafka timestamp vs DB timestamp |
| Database records | 5-10 total | `SELECT COUNT(*) FROM gold.gold_crypto_ohlcv` |
| CPU usage | <30% | `docker stats` |
| Memory usage | <2GB | `docker stats` |
| Dashboard latency | <500ms | Browser developer console |

---

## References

- **Run Application:** See `run.sh --help`
- **Test Application:** See `TEST.md`
- **Production Setup:** See `PRODUCTION.md`
- **All Credentials:** See `CREDENTIALS.md` file

---

**Version:** 1.0.0 | **Status:** Production Ready ✅
