#!/bin/bash

# ============================================================================
# IE212 CRYPTO PIPELINE - QUICK REFERENCE CARD
# Print this for easy reference while running the demo
# ============================================================================

cat << 'EOF'

╔════════════════════════════════════════════════════════════════════════╗
║              IE212 CRYPTO PIPELINE - QUICK REFERENCE                  ║
║                        Cut & Keep This Card                           ║
╚════════════════════════════════════════════════════════════════════════╝

🔐 CREDENTIALS
═══════════════════════════════════════════════════════════════════════════

PostgreSQL Database:
  • Host: localhost
  • Port: 5432
  • Database: cryptopipeline
  • Username: crypto_user
  • Password: crypto_password

Airflow Web UI:
  • Username: airflow
  • Password: airflow

Dashboard:
  • No login required (read-only)

═══════════════════════════════════════════════════════════════════════════

🚀 QUICK START (4 COMMANDS)
═══════════════════════════════════════════════════════════════════════════

1. Navigate to project:
   cd /home/khoa/DoAnBigData/IE212-BigData-CryptoPipeline

2. Start application (Terminal 1):
   ./run.sh --dev --duration 5

3. Monitor in real-time (Terminal 2):
   ./monitor.sh

4. Open browser:
   http://localhost:8501

═══════════════════════════════════════════════════════════════════════════

🌐 URLS
═══════════════════════════════════════════════════════════════════════════

Dashboard (Real-time Charts & KPIs):
  http://localhost:8501

Spark Master (Job Monitoring):
  http://localhost:4040

Airflow (Workflow Management):
  http://localhost:8888
  Login: airflow / airflow

═══════════════════════════════════════════════════════════════════════════

💻 DATABASE COMMANDS
═══════════════════════════════════════════════════════════════════════════

Connect to database:
  docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline

View total records:
  SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;

View latest 10 records:
  SELECT timestamp, symbol, open, close, volume 
  FROM gold.gold_crypto_ohlcv 
  ORDER BY timestamp DESC LIMIT 10;

Check data quality:
  SELECT COUNT(*), 
         COUNT(CASE WHEN error_flag THEN 1 END) as errors
  FROM gold.gold_crypto_ohlcv;

═══════════════════════════════════════════════════════════════════════════

📊 MONITORING COMMANDS
═══════════════════════════════════════════════════════════════════════════

Interactive Monitor:
  ./monitor.sh

Watch Kafka messages (raw ticks):
  docker-compose exec kafka-0 kafka-console-consumer \
    --bootstrap-server kafka-0:9092 \
    --topic crypto_ticks --from-beginning

Watch database growth:
  watch -n 1 'docker-compose exec -T postgres psql -U crypto_user \
    -d cryptopipeline -c "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;"'

Watch Spark processing:
  docker-compose logs -f spark-master | grep -i batch

═══════════════════════════════════════════════════════════════════════════

🛑 STOP / CONTROL
═══════════════════════════════════════════════════════════════════════════

Stop application:
  Press Ctrl+C in Terminal 1
  OR: ./run.sh --stop
  OR: docker-compose down

View logs:
  ./run.sh --logs

Check status:
  ./run.sh --status
  docker-compose ps

═══════════════════════════════════════════════════════════════════════════

⏱️  EXPECTED TIMELINE (5-Minute Demo)
═══════════════════════════════════════════════════════════════════════════

Min 0:  Services starting, Kafka stabilizing
Min 1:  First data arriving in PostgreSQL
Min 2:  2-3 records in database, dashboard ready
Min 3:  Dashboard showing data, Spark processing
Min 4:  ~4 records, trends visible
Min 5:  Demo ends, final metrics displayed

═══════════════════════════════════════════════════════════════════════════

📈 WHAT YOU'LL SEE
═══════════════════════════════════════════════════════════════════════════

Dashboard Panels:
  Panel 1: Real-time candlesticks (BTC, ETH, SOL - last 30 min)
  Panel 2: Historical 5-min aggregated data (last 4 hours)
  Panel 3: System KPIs (throughput, latency, quality)

Database Growth:
  Starting: 5 records
  After 5 min: 10-15 records (1 new record/min)

Monitor Output:
  - Kafka: Raw JSON tick messages arriving
  - Spark: OHLCV aggregation logs
  - PostgreSQL: INSERT statements and row count

═══════════════════════════════════════════════════════════════════════════

🎯 PERFORMANCE TARGETS
═══════════════════════════════════════════════════════════════════════════

Data Arrival:      1 record per minute
System Latency:    4-6 seconds (ingest → display)
Query Speed:       <100ms
Data Quality:      100% valid
CPU Usage:         ~5-10% per service
Memory Usage:      ~500MB per service

═══════════════════════════════════════════════════════════════════════════

❓ QUICK TROUBLESHOOTING
═══════════════════════════════════════════════════════════════════════════

Dashboard won't load:
  • Wait 30 seconds, then refresh (F5)
  • Check: docker-compose logs dashboard | tail -20

No data in database:
  • Check producer: docker-compose logs producer
  • Check Kafka: ./monitor.sh → option 1
  • Check Spark: docker-compose logs spark-master | grep error

Services won't start:
  • Clean up: docker-compose down --volumes
  • Try again: ./run.sh --dev --duration 5

═══════════════════════════════════════════════════════════════════════════

📚 MORE INFO
═══════════════════════════════════════════════════════════════════════════

Full guides:
  • GETTING_STARTED.md - Complete walkthrough
  • CREDENTIALS.md - All passwords and authentication
  • TECHNICAL_SPEC.md - Architecture and design
  • TROUBLESHOOTING.md - Detailed debug guide

═══════════════════════════════════════════════════════════════════════════

Ready to start?

  cd /home/khoa/DoAnBigData/IE212-BigData-CryptoPipeline
  ./run.sh --dev --duration 5
  
  Then open: http://localhost:8501

═══════════════════════════════════════════════════════════════════════════

EOF
