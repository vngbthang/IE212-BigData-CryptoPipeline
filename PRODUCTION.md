# 📦 Production Deployment Guide

## Overview

This guide ensures the IE212 Big Data Crypto Pipeline is production-ready before deployment.

**Estimated completion time:** 4-6 hours  
**Required personnel:** DevOps Lead, Data Engineer, QA Lead  

---

## Pre-Deployment Validation (48 hours before launch)

### Infrastructure & Resource Planning

#### Storage Capacity
- [ ] Database: Verify PostgreSQL storage ≥ 500GB (daily crypto ticks)
- [ ] HDFS: Verify namenode storage ≥ 2TB (historical data archive)
- [ ] Log volumes: Plan for ≥100GB/month (application & system logs)
- [ ] Check: `df -h` on all production servers

#### Network & Connectivity
- [ ] Kafka broker latency <5ms between nodes (intra-datacenter)
- [ ] PostgreSQL connectivity from Spark cluster <10ms
- [ ] Dashboard server connectivity to PostgreSQL <20ms
- [ ] Test with: `ping`, `nc -zv`, connection pool tests

#### Memory & CPU Allocation
- [ ] Spark driver: ≥4GB available (was 1GB in dev)
- [ ] Spark executors: ≥2GB per executor (was 1GB in dev)
- [ ] PostgreSQL buffer pool: 25% of available RAM (typical: 8-16GB for 32GB server)
- [ ] Kafka brokers: ≥4GB heap memory per broker
- [ ] Dashboard (Streamlit): ≥1GB dedicated

#### High Availability Setup
- [ ] Kafka: Validate 3+ broker quorum (replication-factor ≥2)
- [ ] PostgreSQL: Configure streaming replication or backup standby
- [ ] Spark: Verify cluster mode with ≥2 workers for failover
- [ ] Check status: `docker-compose ps` shows all replicas

---

## Software Stack Verification

### Version Compatibility
- [ ] Spark: ≥3.2.0 (for Kafka 7.3 compatibility)
- [ ] PostgreSQL: ≥12 (use 13+ for better performance)
- [ ] Kafka: ≥7.3.0 (Confluent platform)
- [ ] Python: ≥3.9 (for producer/dashboard)
- [ ] Docker: ≥20.10, Docker Compose ≥2.0

### Required Libraries
- [ ] Spark SQL: `spark-sql-kafka-0-10_2.12:3.2.0` installed
- [ ] Spark JDBC: PostgreSQL JDBC driver in Spark classpath
- [ ] Python kafka-python: ≥2.0.2
- [ ] Python streamlit: ≥1.0.0
- [ ] Python pandas: ≥1.3.0
- [ ] Python psycopg2: ≥2.9.0

### System Dependencies
- [ ] Java: ≥JDK 11 (for Spark)
- [ ] ODBC/JDBC drivers: PostgreSQL driver ≥42.2.0
- [ ] Timezone: Verify all systems use UTC or consistent TZ
- [ ] NTP: Sync clock on all nodes to <100ms deviation

---

## Database & Schema Validation

### PostgreSQL Schema
- [ ] Run `sql/init.sql` on production database
- [ ] Verify `gold.gold_crypto_ohlcv` table exists
- [ ] Verify all 24 columns exist and have correct types

```sql
\d gold.gold_crypto_ohlcv
-- Check: open/high/low/close/volume are NUMERIC(20,8)
-- Check: timestamp fields are TIMESTAMPTZ
-- Check: error_flag is BOOLEAN, error_messages is TEXT
```

### Verify All 9 Composite Indexes
```sql
SELECT indexname FROM pg_indexes 
WHERE tablename = 'gold_crypto_ohlcv';
-- Should show ~9 indexes
```

### Partitioning Strategy
- [ ] Verify table is partitioned by DATE (range partitioning)
- [ ] Confirm partition list covers deployment dates
- [ ] Create script for future partition creation

### Backup Configuration
- [ ] Schedule daily full backups (11 PM UTC, off-peak)
- [ ] Enable WAL archiving to separate storage
- [ ] Test restore procedure on staging environment
- [ ] Retention policy: Keep last 30 days of daily backups

---

## Security Hardening

### Access Control
```bash
# Verify credentials are NOT in docker-compose.yml or .env
grep -r "password=" docker-compose.yml  # Should be empty
grep -r "DB_PASS=" .env                 # Should only show expected values

# Change default passwords before production
# Old: crypto_user / crypto_password
# New: Use generated 32-char passwords
psql -h localhost -U postgres -c "ALTER USER crypto_user WITH PASSWORD 'NEW_SECURE_PASSWORD';"
```

### Network Security
- [ ] Configure firewall rules for Kafka brokers (port 9092 internal only)
- [ ] PostgreSQL: Restrict to Spark/Dashboard networks only
- [ ] Dashboard: Use HTTPS with valid certificate
- [ ] Kafka: Enable SASL/SSL for broker authentication

### SSL/TLS Certificates
- [ ] Generate self-signed cert for dev/staging:
```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes
```
- [ ] Use CA-signed cert for production
- [ ] Update docker-compose.yml to mount certificates

---

## Load Testing & Performance

### Baseline Performance
- [ ] Run smoke tests: `./smoke_tests_merged.sh`
- [ ] Baseline metrics:
  - Kafka ingest latency: <100ms per message
  - Spark processing latency: <1s per batch
  - Database write latency: <50ms per record
  - Dashboard query latency: <500ms for 30-min window

### Load Testing Script
```bash
# Generate 10,000 records over 10 minutes
docker-compose exec producer python3 -c "
import time, random
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['kafka-0:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(10000):
    msg = {
        'symbol': 'BTC-USD',
        'price': 25000 + random.uniform(-100, 100),
        'volume': random.uniform(1, 100),
        'timestamp': int(time.time() * 1000)
    }
    producer.send('crypto_ticks', msg)
    if i % 1000 == 0:
        print(f'Sent {i} records...')
    time.sleep(0.06)  # ~1000 msgs/min

producer.flush()
print('Load test complete!')
"
```

### Monitor During Load Test
```bash
# Terminal 1: Watch logs
docker-compose logs -f spark-master | grep "processed"

# Terminal 2: Monitor database
watch -n 1 "psql -h localhost -U crypto_user -d cryptopipeline -c \
  'SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE timestamp >= NOW() - INTERVAL 10min;'"

# Terminal 3: Monitor Kafka lag
kafka-consumer-groups.sh --bootstrap-server kafka-0:9092 --group spark-group --describe
```

---

## Deployment Steps

### 1. Pre-Deployment (Production Environment)
```bash
# Pull latest code
git pull origin main
git checkout $(git describe --tags --abbrev=0)

# Verify all tests pass
./run_tests.sh --cleanup

# Review config
cat .env | grep -E "CRYPTO_MODE|DB_"
```

### 2. Deploy Services
```bash
# Start all services in production mode
export CRYPTO_MODE=prod
docker-compose up -d

# Wait for initialization
sleep 120

# Verify all services healthy
docker-compose ps  # All should be "Up"

# Check logs for errors
docker-compose logs --tail=50 | grep -i error
```

### 3. Smoke Test Production
```bash
./smoke_tests_merged.sh
```

### 4. Enable Monitoring
```bash
# Start monitoring dashboard
./monitor.sh

# Review key metrics:
# - Message ingest rate (should be ~60 msgs/min in prod)
# - Data arrival latency (should be <5 seconds)
# - Database size growth
# - Error rates (should be 0%)
```

### 5. Enable Logging & Alerting
```bash
# Configure centralized logging (ELK/Splunk/etc)
# Update docker-compose.yml with log drivers

# Set up alerts for:
# - Kafka broker down
# - Spark job failure
# - Database connection errors
# - Dashboard unavailable
```

---

## Rollback Procedure

If production deployment fails:

```bash
# 1. Stop current services
docker-compose down

# 2. Revert to previous version
git checkout <previous-tag>

# 3. Start previous version
docker-compose up -d

# 4. Verify operation
./smoke_tests_merged.sh

# 5. Debug and retry deployment
```

---

## Post-Deployment Monitoring

### Daily Checks
- [ ] Dashboard loads without errors
- [ ] Real-time data visible in charts
- [ ] No error messages in logs
- [ ] Database growth is expected (~1-2 GB/month)
- [ ] Query latency <500ms

### Weekly Checks
- [ ] Run automated tests: `./run_tests.sh`
- [ ] Review error logs for patterns
- [ ] Backup verification (restore test)
- [ ] Capacity planning review
- [ ] Performance trending

### Monthly Checks
- [ ] Full disaster recovery drill
- [ ] Update dependencies
- [ ] Capacity forecast next quarter
- [ ] Cost analysis and optimization
- [ ] Security audit

---

## Rollforward (Updating Production)

```bash
# 1. Tag new version
git tag -a v2.0.0 -m "Production release"
git push origin v2.0.0

# 2. Pre-deployment testing
./run_tests.sh --cleanup

# 3. Load tests (optional)
# See "Load Testing & Performance" section above

# 4. Blue-Green Deployment (recommended)
# Start new services on alternate ports
# Run smoke tests on new environment
# Switch traffic after validation
# Keep old environment running for quick rollback

# 5. Verify deployment
./monitor.sh
```

---

## Contacts & Escalation

- **On-Call Engineer:** See PagerDuty rotation
- **Data Platform Team:** data-platform@company.com
- **Database Admin:** dba@company.com
- **Security Team:** security@company.com

---

**Version:** 1.0.0 | **Status:** Production Ready ✅
