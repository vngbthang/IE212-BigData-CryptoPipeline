#!/bin/bash

# ============================================================================
# IE212 CRYPTO PIPELINE - REAL-TIME MONITOR
# Displays live data flow through all pipeline stages
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

cd /home/khoa/DoAnBigData/IE212-BigData-CryptoPipeline

show_header() {
    clear
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║     IE212 CRYPTO PIPELINE - REAL-TIME MONITOR                         ║${NC}"
    echo -e "${BLUE}║        Press Ctrl+C to exit, R to refresh, 1-3 to select view        ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

view_kafka_messages() {
    show_header
    echo -e "${YELLOW}📨 KAFKA MESSAGES (Live Ticks)${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    echo -e "${CYAN}Latest 10 messages from kafka_ticks topic:${NC}"
    echo ""
    
    docker-compose exec -T kafka-0 kafka-console-consumer \
        --bootstrap-server kafka-0:9092 \
        --topic crypto_ticks \
        --max-messages 10 \
        --from-beginning 2>/dev/null | \
        python3 -m json.tool 2>/dev/null || \
        echo "(Waiting for messages...)"
    
    echo ""
}

view_database_state() {
    show_header
    echo -e "${YELLOW}🗄️  DATABASE STATE (PostgreSQL)${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # Record count
    COUNT=$(docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline -tc \
        "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;" 2>/dev/null || echo "0")
    
    echo -e "${CYAN}Total records:${NC} ${GREEN}$COUNT${NC}"
    echo ""
    
    # Latest records
    echo -e "${CYAN}Latest 10 records:${NC}"
    echo ""
    docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline \
        -c "SELECT 
            TO_CHAR(timestamp, 'HH24:MI:SS') as TIME,
            symbol,
            ROUND(open::numeric, 2) as OPEN,
            ROUND(high::numeric, 2) as HIGH,
            ROUND(low::numeric, 2) as LOW,
            ROUND(close::numeric, 2) as CLOSE,
            ROUND(volume::numeric, 4) as VOL,
            ROUND((close::numeric - open::numeric) / open::numeric * 100, 2) as CHANGE_PCT
        FROM gold.gold_crypto_ohlcv 
        ORDER BY timestamp DESC 
        LIMIT 10;" 2>/dev/null || echo "(Loading data...)"
    
    echo ""
    
    # Data quality
    echo -e "${CYAN}Data Quality:${NC}"
    echo ""
    docker-compose exec -T postgres psql -U crypto_user -d cryptopipeline \
        -c "SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN error_flag THEN 1 END) as error_count,
            ROUND(100.0 * COUNT(CASE WHEN error_flag THEN 1 END) / COUNT(*), 2) as error_pct,
            COUNT(CASE WHEN error_flag = FALSE THEN 1 END) as valid_count
        FROM gold.gold_crypto_ohlcv;" 2>/dev/null || echo "(Calculating...)"
    
    echo ""
}

view_spark_status() {
    show_header
    echo -e "${YELLOW}⚡ SPARK STREAMING STATUS${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    echo -e "${CYAN}Spark Master:${NC} ${BLUE}http://localhost:4040${NC}"
    echo ""
    
    echo -e "${CYAN}Active applications:${NC}"
    echo ""
    curl -s http://localhost:4040/api/v1/applications 2>/dev/null | \
        python3 -m json.tool 2>/dev/null | \
        grep -E '"name"|"status"|"attempts"' || \
        echo "(Spark UI loading...)"
    
    echo ""
    echo -e "${CYAN}Streaming status:${NC}"
    echo ""
    docker-compose logs spark-master 2>/dev/null | \
        grep -i 'batch\|micro\|ohlcv\|processing' | \
        tail -10 || echo "(No streaming logs yet)"
    
    echo ""
}

view_system_health() {
    show_header
    echo -e "${YELLOW}🏥 SYSTEM HEALTH CHECK${NC}"
    echo -e "${YELLOW}═════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # Service status
    echo -e "${CYAN}Service Status:${NC}"
    echo ""
    docker-compose ps | grep -E "postgres|kafka|spark|producer|airflow" | awk '{
        name = $1
        status = $(NF-1)
        
        if (status ~ /Up/) {
            color = "\033[0;32m✓"
        } else {
            color = "\033[0;31m✗"
        }
        
        printf color " %-20s %s\033[0m\n", name, status
    }' || echo "(Services loading...)"
    
    echo ""
    
    # Memory usage
    echo -e "${CYAN}Memory Usage:${NC}"
    echo ""
    docker stats --no-stream --format "table {{.Container}}\t{{.MemUsage}}" 2>/dev/null | \
        grep -E "postgres|kafka|spark" | head -10 || echo "(Stats loading...)"
    
    echo ""
    
    # Port availability
    echo -e "${CYAN}Available Services:${NC}"
    echo ""
    
    if curl -s http://localhost:8501 > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} Dashboard: ${BLUE}http://localhost:8501${NC}"
    else
        echo -e "  ${RED}✗${NC} Dashboard: ${BLUE}http://localhost:8501${NC} (not ready)"
    fi
    
    if curl -s http://localhost:4040 > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} Spark UI: ${BLUE}http://localhost:4040${NC}"
    else
        echo -e "  ${RED}✗${NC} Spark UI: ${BLUE}http://localhost:4040${NC} (not ready)"
    fi
    
    if curl -s http://localhost:8888 > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} Airflow: ${BLUE}http://localhost:8888${NC}"
    else
        echo -e "  ${RED}✗${NC} Airflow: ${BLUE}http://localhost:8888${NC} (not ready)"
    fi
    
    echo ""
}

main_menu() {
    show_header
    echo -e "${CYAN}Select monitoring view:${NC}"
    echo ""
    echo -e "  ${GREEN}1${NC}) Kafka Messages (Live Ticks)"
    echo -e "  ${GREEN}2${NC}) Database State (Latest Records)"
    echo -e "  ${GREEN}3${NC}) Spark Streaming Status"
    echo -e "  ${GREEN}4${NC}) System Health Check"
    echo -e "  ${GREEN}5${NC}) Auto-refresh all (10 sec intervals)"
    echo -e "  ${GREEN}0${NC}) Exit"
    echo ""
    echo -ne "${YELLOW}Choose view (0-5):${NC} "
    read -r choice
    
    case $choice in
        1) view_kafka_messages ;;
        2) view_database_state ;;
        3) view_spark_status ;;
        4) view_system_health ;;
        5)
            while true; do
                view_database_state
                echo -e "${YELLOW}(Auto-refreshing in 10 seconds, Ctrl+C to stop)${NC}"
                sleep 10
            done
            ;;
        0) exit 0 ;;
        *) main_menu ;;
    esac
    
    echo ""
    echo -ne "${YELLOW}Press Enter to continue...${NC}"
    read
    main_menu
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main_menu
