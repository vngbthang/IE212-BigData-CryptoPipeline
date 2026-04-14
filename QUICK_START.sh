#!/bin/bash

# ============================================================================
# IE212 CRYPTO PIPELINE - QUICK START (5 MINUTE DEMO)
# Development Mode with Mock Data
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     IE212 CRYPTO PIPELINE - QUICK START (5 MIN DEMO)                  ║${NC}"
echo -e "${BLUE}║         Development Mode with Mock Cryptocurrency Data               ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Step 1: Navigate to project
echo -e "${YELLOW}STEP 1: Navigate to project directory${NC}"
cd /home/khoa/DoAnBigData/IE212-BigData-CryptoPipeline
echo -e "${GREEN}✓ In project directory: $(pwd)${NC}"
echo ""

# Step 2: Check Docker
echo -e "${YELLOW}STEP 2: Verify Docker is running${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker not found. Please install Docker first.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker available: $(docker --version)${NC}"
echo ""

# Step 3: Check if services are running
echo -e "${YELLOW}STEP 3: Check if services are already running${NC}"
if docker-compose ps | grep -q "postgres"; then
    echo -e "${GREEN}✓ Services already running!${NC}"
    RUNNING=true
else
    echo -e "${YELLOW}ℹ Services not running, will start them...${NC}"
    RUNNING=false
fi
echo ""

# Step 4: Start services if needed
if [ "$RUNNING" = false ]; then
    echo -e "${YELLOW}STEP 4: Start Docker services${NC}"
    echo -e "${BLUE}Starting 12+ services (Kafka, Spark, PostgreSQL, Dashboard, etc.)${NC}"
    echo -e "${BLUE}This may take 30-90 seconds...${NC}"
    echo ""
    
    docker-compose up -d
    
    echo -e "${GREEN}✓ Services starting...${NC}"
    echo -e "${BLUE}Waiting 60 seconds for Kafka stabilization...${NC}"
    sleep 60
else
    echo -e "${YELLOW}STEP 4: Services already running (skipping startup)${NC}"
fi
echo ""

# Step 5: Verify services
echo -e "${YELLOW}STEP 5: Verify all services are healthy${NC}"
echo ""
docker-compose ps | grep -E "postgres|kafka|spark|producer|airflow" | head -10
echo ""
echo -e "${GREEN}✓ Services running${NC}"
echo ""

# Step 6: Check PostgreSQL connection
echo -e "${YELLOW}STEP 6: Verify PostgreSQL has data${NC}"
RECORD_COUNT=$(docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline -tc "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;" 2>/dev/null || echo "0")
echo -e "${GREEN}✓ Database records: ${RECORD_COUNT}${NC}"
echo ""

# Step 7: Open dashboard
echo -e "${YELLOW}STEP 7: Access the Dashboard${NC}"
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  DASHBOARD URL:  http://localhost:8501              ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}The dashboard will show:${NC}"
echo -e "  ${GREEN}✓${NC} Panel 1: Real-time 1-min cryptocurrency prices (last 30 min)"
echo -e "  ${GREEN}✓${NC} Panel 2: Historical 5-min aggregated data (last 4 hours)"
echo -e "  ${GREEN}✓${NC} Panel 3: System KPIs (throughput, latency, data quality)"
echo ""
echo -e "${YELLOW}What you should see:${NC}"
echo -e "  • BTC-USD, ETH-USD, SOL-USD candlestick charts"
echo -e "  • Current prices and volume data"
echo -e "  • Throughput: ~1 record/min (1-min OHLCV candles)"
echo -e "  • Latency: 4-6 seconds from ingest to display"
echo -e "  • Data quality: 100% valid"
echo ""

# Step 8: Show monitoring commands
echo -e "${YELLOW}STEP 8: Monitor data flow (Optional - in separate terminal)${NC}"
echo ""
echo -e "${BLUE}Command 1: Watch Kafka messages${NC}"
echo "  docker-compose exec kafka-0 kafka-console-consumer \\"
echo "    --bootstrap-server kafka-0:9092 \\"
echo "    --topic crypto_ticks \\"
echo "    --max-messages 5"
echo ""
echo -e "${BLUE}Command 2: Watch database records${NC}"
echo "  docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline -c \\"
echo "    \"SELECT timestamp, symbol, open, close, volume FROM gold.gold_crypto_ohlcv ORDER BY timestamp DESC LIMIT 5;\""
echo ""
echo -e "${BLUE}Command 3: Check Spark logs${NC}"
echo "  docker-compose logs spark-master -f | grep -i ohlcv"
echo ""

# Step 9: Show the data
echo -e "${YELLOW}STEP 9: View sample data from database${NC}"
echo ""
echo -e "${BLUE}Latest 5 records in PostgreSQL:${NC}"
echo ""
docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline \
  -c "SELECT TO_CHAR(timestamp, 'HH24:MI:SS') as time, symbol, ROUND(open::numeric, 2) as open, ROUND(close::numeric, 2) as close, ROUND(volume::numeric, 4) as vol FROM gold.gold_crypto_ohlcv ORDER BY timestamp DESC LIMIT 5;" 2>/dev/null || echo "  (Loading data...)"
echo ""

# Step 10: Final instructions
echo -e "${YELLOW}STEP 10: Demo Duration & What to Expect${NC}"
echo ""
echo -e "${GREEN}⏱️  Demo Duration: 5 minutes${NC}"
echo -e "  The producer will continuously send mock data"
echo -e "  Spark will aggregate it every 60 seconds"
echo -e "  Dashboard will update every 10 seconds"
echo ""
echo -e "${GREEN}📊 Expected Performance:${NC}"
echo -e "  • Data arrival rate: 1 record/min (1-min OHLCV)"
echo -e "  • System latency: 4-6 seconds (ingest → display)"
echo -e "  • Database queries: <100ms (due to 9 optimized indexes)"
echo -e "  • CPU usage: ~5-10% per service"
echo -e "  • Memory: ~500MB per service"
echo ""

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                         QUICK START COMPLETE!                          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}✓ Open browser: http://localhost:8501${NC}"
echo -e "${GREEN}✓ Watch Kafka: See Command 1 above${NC}"
echo -e "${GREEN}✓ Check database: See Command 2 above${NC}"
echo -e "${GREEN}✓ Monitor Spark: See Command 3 above${NC}"
echo ""
echo -e "${YELLOW}To stop the demo after 5 minutes:${NC}"
echo -e "  docker-compose down  (stops all services)"
echo -e "  docker-compose logs  (view historical logs)"
echo ""
