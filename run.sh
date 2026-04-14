#!/bin/bash

# ============================================================================
# IE212 CRYPTO PIPELINE - RUN APPLICATION
# Starts the pipeline and displays real-time output
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Default values
MODE="dev"
DURATION=5
ACTION="start"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --prod)
            MODE="prod"
            shift
            ;;
        --dev)
            MODE="dev"
            shift
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --stop)
            ACTION="stop"
            shift
            ;;
        --logs)
            ACTION="logs"
            shift
            ;;
        --status)
            ACTION="status"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--dev|--prod] [--duration MINUTES] [--stop|--logs|--status]"
            exit 1
            ;;
    esac
done

cd /home/khoa/DoAnBigData/IE212-BigData-CryptoPipeline

# ============================================================================
# FUNCTIONS
# ============================================================================

show_header() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║     IE212 CRYPTO PIPELINE - APPLICATION RUNNER                        ║${NC}"
    echo -e "${BLUE}║              Mode: ${YELLOW}$MODE${BLUE} | Duration: ${YELLOW}${DURATION}${BLUE} min                       ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

start_services() {
    echo -e "${YELLOW}Starting Docker services...${NC}"
    
    if docker-compose ps | grep -q "postgres.*Up"; then
        echo -e "${GREEN}✓ Services already running${NC}"
    else
        echo -e "${BLUE}Pulling images and starting containers...${NC}"
        docker-compose up -d
        echo -e "${BLUE}Waiting 90 seconds for Kafka stabilization...${NC}"
        sleep 90
    fi
    
    echo -e "${GREEN}✓ All services started${NC}"
    echo ""
}

show_status() {
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}SERVICE STATUS${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    docker-compose ps | grep -E "postgres|kafka|spark|producer|airflow|dashboard" || true
    echo ""
}

show_database_info() {
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}DATABASE STATUS${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # Count records
    COUNT=$(docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline -tc "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;" 2>/dev/null || echo "0")
    echo -e "${GREEN}Total records in database:${NC} $COUNT"
    
    # Show latest records
    echo ""
    echo -e "${GREEN}Latest 5 records:${NC}"
    echo ""
    docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline -c \
        "SELECT TO_CHAR(timestamp, 'HH24:MI:SS') as time, symbol, ROUND(open::numeric, 2) as open, ROUND(close::numeric, 2) as close, ROUND(volume::numeric, 4) as vol FROM gold.gold_crypto_ohlcv ORDER BY timestamp DESC LIMIT 5;" 2>/dev/null || echo "(Loading data...)"
    
    echo ""
}

show_dashboard_instructions() {
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}DASHBOARD & MONITORING${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    echo -e "${GREEN}📊 DASHBOARD (Open in Browser):${NC}"
    echo -e "   ${BLUE}http://localhost:8501${NC}"
    echo ""
    
    echo -e "${GREEN}📊 SPARK UI (Monitor Streaming):${NC}"
    echo -e "   ${BLUE}http://localhost:4040${NC}"
    echo ""
    
    echo -e "${GREEN}📊 AIRFLOW UI (Workflow Management):${NC}"
    echo -e "   ${BLUE}http://localhost:8888${NC}"
    echo ""
    
    echo -e "${GREEN}💬 What You'll See in Dashboard:${NC}"
    echo "   • Panel 1: Real-time 1-min cryptocurrency candlesticks (BTC, ETH, SOL)"
    echo "   • Panel 2: Historical 5-min aggregated data (last 4 hours)"
    echo "   • Panel 3: System KPIs (throughput, latency, data quality)"
    echo ""
}

show_monitoring_commands() {
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}MONITORING COMMANDS (Run in separate terminal)${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    echo -e "${GREEN}🔍 Watch Kafka messages in real-time:${NC}"
    echo "   ${BLUE}docker-compose exec kafka-0 kafka-console-consumer --bootstrap-server kafka-0:9092 --topic crypto_ticks --from-beginning${NC}"
    echo ""
    
    echo -e "${GREEN}📊 Watch database records grow:${NC}"
    echo "   ${BLUE}watch -n 1 'docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline -c \"SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;\"'${NC}"
    echo ""
    
    echo -e "${GREEN}📈 Watch Spark streaming:${NC}"
    echo "   ${BLUE}docker-compose logs -f spark-master | grep -i 'batch\\|row\\|ohlcv'${NC}"
    echo ""
    
    echo -e "${GREEN}📋 Watch producer logs:${NC}"
    echo "   ${BLUE}docker-compose logs -f producer${NC}"
    echo ""
    
    echo -e "${GREEN}🐘 Check PostgreSQL queries:${NC}"
    echo "   ${BLUE}docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline${NC}"
    echo "   Then: ${YELLOW}SELECT * FROM gold.gold_crypto_ohlcv ORDER BY timestamp DESC LIMIT 10;${NC}"
    echo ""
}

show_performance_metrics() {
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}EXPECTED PERFORMANCE METRICS${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    echo -e "${GREEN}⏱️  Latency (Ingest → Display):${NC} 4-6 seconds"
    echo -e "${GREEN}📊 Throughput:${NC} 1 record/minute (1-min OHLCV candles)"
    echo -e "${GREEN}🔍 Query Response Time:${NC} <100ms (due to 9 optimized indexes)"
    echo -e "${GREEN}✓ Data Quality:${NC} 100% valid records"
    echo -e "${GREEN}💾 Memory per Service:${NC} ~500MB"
    echo -e "${GREEN}⚙️  CPU per Service:${NC} ~5-10%"
    echo ""
}

show_stopping_instructions() {
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}STOPPING THE APPLICATION${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    echo -e "${GREEN}To stop after ${DURATION} minutes:${NC}"
    echo "   ${BLUE}docker-compose down${NC}"
    echo ""
    
    echo -e "${GREEN}To view logs after stopping:${NC}"
    echo "   ${BLUE}docker-compose logs${NC}"
    echo ""
    
    echo -e "${GREEN}To clean up volumes (careful!):${NC}"
    echo "   ${BLUE}docker-compose down --volumes${NC}"
    echo ""
}

view_logs() {
    echo -e "${YELLOW}Showing logs (Ctrl+C to exit):${NC}"
    echo ""
    docker-compose logs -f --tail=100
}

stop_services() {
    echo -e "${YELLOW}Stopping all services...${NC}"
    docker-compose down
    echo -e "${GREEN}✓ Services stopped${NC}"
    echo ""
    echo -e "${YELLOW}To restart: $0 --dev (or --prod)${NC}"
    echo ""
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

show_header

case $ACTION in
    start)
        start_services
        show_status
        show_database_info
        show_dashboard_instructions
        show_monitoring_commands
        show_performance_metrics
        show_stopping_instructions
        
        echo -e "${GREEN}╔════════════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║                  APPLICATION READY TO USE!                             ║${NC}"
        echo -e "${GREEN}║                                                                        ║${NC}"
        echo -e "${GREEN}║  1. Open browser: ${BLUE}http://localhost:8501${GREEN}                              ║${NC}"
        echo -e "${GREEN}║  2. Use monitoring commands above to watch data flow                   ║${NC}"
        echo -e "${GREEN}║  3. Dashboard updates every 10 seconds                                 ║${NC}"
        echo -e "${GREEN}║  4. New data arrives every 60 seconds (1-min OHLCV)                    ║${NC}"
        echo -e "${GREEN}║  5. After ${DURATION} min: run 'docker-compose down'                             ║${NC}"
        echo -e "${GREEN}╚════════════════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        
        echo -e "${YELLOW}Application running for ${DURATION} minutes...${NC}"
        echo -e "${YELLOW}Press Ctrl+C to stop early, or wait for timeout${NC}"
        echo ""
        
        # Run for specified duration
        sleep $((DURATION * 60))
        
        echo -e "${YELLOW}Demo time complete! Showing final database state...${NC}"
        show_database_info
        ;;
    
    stop)
        stop_services
        ;;
    
    logs)
        view_logs
        ;;
    
    status)
        show_status
        show_database_info
        ;;
esac
