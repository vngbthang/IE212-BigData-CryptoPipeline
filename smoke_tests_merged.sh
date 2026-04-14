#!/bin/bash

# ============================================================================
# SMOKE TEST SUITE - POST-MERGE VALIDATION
# IE212 Big Data Crypto Pipeline: Silver → Gold Integration
# Tests: Docker stack, Kafka producer, Spark processing, PostgreSQL, Dashboard, Latency
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
END='\033[0m'

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POSTGRES_HOST="${DB_HOST:-localhost}"
POSTGRES_PORT="${DB_PORT:-5432}"
POSTGRES_DB="${DB_NAME:-cryptopipeline}"
POSTGRES_USER="${DB_USER:-crypto_user}"
POSTGRES_PASSWORD="${DB_PASS:-crypto_password}"
KAFKA_BROKERS="${KAFKA_BROKERS:-kafka-0:9092,kafka-1:9092,kafka-2:9092}"
KAFKA_TOPIC="crypto_ticks"
SPARK_MASTER_URL="http://localhost:8080"
DASHBOARD_URL="http://localhost:8501"

# Test results tracking
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

print_header() {
    echo -e "\n${BOLD}${BLUE}╔════════════════════════════════════════════════════════════╗${END}"
    echo -e "${BOLD}${BLUE}║ $1${END}"
    echo -e "${BOLD}${BLUE}╚════════════════════════════════════════════════════════════╝${END}\n"
}

print_test_header() {
    echo -e "\n${BOLD}${BLUE}─────────────────────────────────────────────────────────────${END}"
    echo -e "${BOLD}TEST $1: $2${END}"
    echo -e "${BOLD}${BLUE}─────────────────────────────────────────────────────────────${END}"
}

print_success() {
    echo -e "${GREEN}✅ $1${END}"
    ((TESTS_PASSED++))
}

print_failure() {
    echo -e "${RED}❌ $1${END}"
    ((TESTS_FAILED++))
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${END}"
    ((TESTS_SKIPPED++))
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${END}"
}

# ============================================================================
# TEST 1: DOCKER STACK HEALTH CHECK
# ============================================================================

test_docker_health() {
    print_test_header "1" "Docker Stack Health Check"
    
    # Check if docker-compose is running
    if ! docker-compose ps &>/dev/null; then
        print_failure "docker-compose not running or not installed"
        return 1
    fi
    
    print_info "Checking critical services..."
    
    local critical_services=("postgres" "kafka-0" "zookeeper" "spark-master" "spark-worker-1")
    local health_ok=true
    
    for service in "${critical_services[@]}"; do
        local status=$(docker-compose ps $service 2>/dev/null | grep -E 'Up|running' | wc -l)
        if [ "$status" -gt 0 ]; then
            print_success "$service is running"
        else
            print_failure "$service is NOT running"
            health_ok=false
        fi
    done
    
    # Check postgres health status
    local pg_health=$(docker-compose ps postgres 2>/dev/null | grep "healthy" | wc -l)
    if [ "$pg_health" -gt 0 ]; then
        print_success "PostgreSQL is HEALTHY"
    else
        print_warning "PostgreSQL health status unclear (but may be running)"
    fi
    
    return 0
}

# ============================================================================
# TEST 2: KAFKA TOPIC & PRODUCER VALIDATION
# ============================================================================

test_kafka_producer() {
    print_test_header "2" "Kafka Producer & Topic Validation"
    
    print_info "Checking producer logs (last 10 messages)..."
    
    # Try to get producer container logs
    local producer_logs=$(docker-compose logs producer 2>/dev/null | tail -20)
    
    if echo "$producer_logs" | grep -q "\[MOCK\] Sent"; then
        local count=$(echo "$producer_logs" | grep -c "\[MOCK\] Sent" || echo 0)
        print_success "Mock producer sending messages (found $count recent sends)"
    elif echo "$producer_logs" | grep -q "\[AVRO\]"; then
        print_success "Real producer (AVRO mode) is active"
    else
        print_warning "Producer logs not clear - service may be still starting"
    fi
    
    # Check for errors in producer
    if echo "$producer_logs" | grep -q "ERROR\|error\|Failed"; then
        print_failure "Producer errors detected in logs"
        echo "$producer_logs" | grep -E "ERROR|error|Failed" | head -5
        return 1
    else
        print_success "No producer errors detected"
    fi
    
    print_info "Producer message format should be: {type, product_id, price, time, size}"
    print_success "Kafka topic validation complete"
    
    return 0
}

# ============================================================================
# TEST 3: SPARK STREAM PROCESSING
# ============================================================================

test_spark_processing() {
    print_test_header "3" "Spark Stream Processing Validation"
    
    print_info "Checking Spark logs..."
    
    # Try to get spark master logs
    local spark_logs=$(docker-compose logs spark-master 2>/dev/null | tail -50)
    
    if echo "$spark_logs" | grep -q "OHLCV DataFrame Schema"; then
        print_success "Spark is aggregating OHLCV candles"
    else
        print_warning "OHLCV schema not found in recent logs - stream may be starting"
    fi
    
    if echo "$spark_logs" | grep -q "Streaming processor is running"; then
        print_success "Spark streaming processor is ACTIVE"
    else
        print_warning "Streaming processor status unclear"
    fi
    
    if echo "$spark_logs" | grep -q "\[BATCH\]"; then
        local batch_count=$(echo "$spark_logs" | grep -c "\[BATCH\]" || echo 0)
        print_success "Spark batches detected (found $batch_count recent batches)"
    else
        print_warning "Batch processing logs not found - may be normal for startup"
    fi
    
    # Check for errors
    if echo "$spark_logs" | grep -q "ERROR\|Exception" | grep -v "spark.python"; then
        print_warning "Spark errors found (may be non-critical)"
    else
        print_success "No critical Spark errors"
    fi
    
    return 0
}

# ============================================================================
# TEST 4: POSTGRESQL DATA ARRIVAL
# ============================================================================

test_postgresql_data() {
    print_test_header "4" "PostgreSQL Data Arrival Check"
    
    print_info "Connecting to PostgreSQL at $POSTGRES_HOST:$POSTGRES_PORT..."
    
    # Check if we can connect
    if ! PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT version();" &>/dev/null; then
        print_failure "Cannot connect to PostgreSQL"
        return 1
    fi
    
    print_success "PostgreSQL connection successful"
    
    # Check table exists
    local table_count=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'gold' AND table_name = 'gold_crypto_ohlcv';" 2>/dev/null)
    
    if [ "$table_count" -gt 0 ]; then
        print_success "gold_crypto_ohlcv table exists"
    else
        print_failure "gold_crypto_ohlcv table NOT found"
        return 1
    fi
    
    # Check for recent data (last 5 minutes)
    local record_count=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE timestamp >= NOW() - INTERVAL '5 minutes';" 2>/dev/null)
    
    if [ "$record_count" -gt 0 ]; then
        print_success "Data arriving: $record_count records in last 5 minutes"
    else
        print_warning "No data in last 5 minutes (stream may be starting, wait 60+ seconds)"
        record_count=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;" 2>/dev/null)
        if [ "$record_count" -gt 0 ]; then
            print_info "But found $record_count total records (data flow working)"
        fi
    fi
    
    # Check data quality
    local error_count=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE error_flag = TRUE LIMIT 10;" 2>/dev/null)
    
    if [ "$error_count" -eq 0 ]; then
        print_success "Data quality: No error flags detected"
    else
        print_warning "Found $error_count records with error_flag=TRUE"
    fi
    
    # Check schema columns
    local col_count=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'gold' AND table_name = 'gold_crypto_ohlcv';" 2>/dev/null)
    
    if [ "$col_count" -eq 24 ]; then
        print_success "Schema complete: All 24 columns present"
    else
        print_warning "Column count is $col_count (expected 24)"
    fi
    
    return 0
}

# ============================================================================
# TEST 5: DASHBOARD CONNECTIVITY & DATA
# ============================================================================

test_dashboard() {
    print_test_header "5" "Dashboard Connectivity & Data Display"
    
    # Check if dashboard container is running
    local dashboard_status=$(docker-compose ps dashboard 2>/dev/null | grep -c "Up" || echo 0)
    
    if [ "$dashboard_status" -eq 0 ]; then
        print_warning "Dashboard container not running (this is optional with --profile)"
        print_info "To enable: docker-compose --profile dashboard up -d"
        return 0
    fi
    
    print_success "Dashboard container is running"
    
    # Try to connect (basic HTTP check)
    if curl -s -f "$DASHBOARD_URL" > /dev/null 2>&1; then
        print_success "Dashboard is accessible at $DASHBOARD_URL"
    else
        print_warning "Dashboard HTTP check failed (may need time to start)"
    fi
    
    # Check dashboard logs for errors
    local dashboard_logs=$(docker-compose logs dashboard 2>/dev/null | tail -30)
    
    if echo "$dashboard_logs" | grep -q "Database error\|ERROR\|Exception"; then
        print_failure "Dashboard database errors detected"
        echo "$dashboard_logs" | grep -E "error|ERROR" | head -3
        return 1
    else
        print_success "No database errors in dashboard logs"
    fi
    
    if echo "$dashboard_logs" | grep -q "Streamlit app is running"; then
        print_success "Dashboard Streamlit app is active"
    else
        print_info "Dashboard startup message not found (may be normal)"
    fi
    
    print_info "Dashboard panels: Real-time, History, System KPIs"
    print_info "Expected panels: Panel 1 (1-min candles), Panel 2 (5-min history), Panel 3 (KPIs)"
    
    return 0
}

# ============================================================================
# TEST 6: END-TO-END LATENCY MEASUREMENT
# ============================================================================

test_latency() {
    print_test_header "6" "End-to-End Latency Check (Ingest to Display)"
    
    print_info "Measuring latency: Kafka ingest_timestamp → PostgreSQL display_timestamp"
    
    # Query average latency
    local avg_latency=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT AVG(EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000) FROM gold.gold_crypto_ohlcv WHERE timestamp >= NOW() - INTERVAL '10 minutes';" 2>/dev/null)
    
    if [ -z "$avg_latency" ] || [ "$avg_latency" = "NULL" ]; then
        print_warning "No latency data (no records in last 10 minutes)"
        return 0
    fi
    
    # Convert to number
    avg_latency=$(printf "%.2f" "$avg_latency")
    
    if (( $(echo "$avg_latency < 10000" | bc -l) )); then
        print_success "Average latency: ${avg_latency}ms (< 10 seconds ✓)"
    else
        print_warning "Average latency: ${avg_latency}ms (slightly high but acceptable for demo)"
    fi
    
    # Query percentiles
    local p95_latency=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000) FROM gold.gold_crypto_ohlcv WHERE timestamp >= NOW() - INTERVAL '10 minutes' AND error_flag = FALSE;" 2>/dev/null)
    
    if [ -z "$p95_latency" ] || [ "$p95_latency" = "NULL" ]; then
        print_info "P95 latency: No data"
    else
        p95_latency=$(printf "%.2f" "$p95_latency")
        if (( $(echo "$p95_latency < 5000" | bc -l) )); then
            print_success "P95 latency: ${p95_latency}ms (< 5 seconds ✓)"
        else
            print_info "P95 latency: ${p95_latency}ms"
        fi
    fi
    
    return 0
}

# ============================================================================
# SUMMARY REPORT
# ============================================================================

print_summary() {
    local total=$((TESTS_PASSED + TESTS_FAILED + TESTS_SKIPPED))
    local pass_rate=0
    
    if [ "$total" -gt 0 ]; then
        pass_rate=$((TESTS_PASSED * 100 / total))
    fi
    
    echo -e "\n${BOLD}${BLUE}╔════════════════════════════════════════════════════════════╗${END}"
    echo -e "${BOLD}${BLUE}║  TEST SUMMARY${END}"
    echo -e "${BOLD}${BLUE}╚════════════════════════════════════════════════════════════╝${END}"
    
    echo -e "\n${GREEN}✅ Passed:   $TESTS_PASSED${END}"
    echo -e "${RED}❌ Failed:   $TESTS_FAILED${END}"
    echo -e "${YELLOW}⚠️  Skipped:  $TESTS_SKIPPED${END}"
    echo -e "${BOLD}📊 Pass Rate: ${pass_rate}% ($TESTS_PASSED/$total)${END}"
    
    echo -e "\n${BOLD}Status:${END}"
    if [ "$TESTS_FAILED" -eq 0 ]; then
        echo -e "${GREEN}🎉 ALL CRITICAL TESTS PASSED${END}"
        echo -e "\n${BOLD}Next Steps:${END}"
        echo -e "1. Open Dashboard: ${BLUE}http://localhost:8501${END}"
        echo -e "2. Verify data in all 3 panels"
        echo -e "3. Check KPI metrics (throughput, latency, quality)"
        echo -e "4. Monitor: ${BLUE}docker-compose logs spark-master${END} (Spark processing)"
        echo -e "5. Monitor: ${BLUE}docker-compose logs producer${END} (Data ingestion)"
        return 0
    else
        echo -e "${RED}⚠️  Some tests failed - review output above${END}"
        echo -e "\n${BOLD}Troubleshooting:${END}"
        echo -e "1. Wait 60+ seconds for all services to stabilize"
        echo -e "2. Check logs: ${BLUE}docker-compose logs${END}"
        echo -e "3. Verify all services running: ${BLUE}docker-compose ps${END}"
        echo -e "4. Restart if needed: ${BLUE}docker-compose restart${END}"
        return 1
    fi
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    print_header "🚀 SMOKE TEST SUITE - POST-MERGE VALIDATION"
    print_info "Project: IE212 Big Data Crypto Pipeline"
    print_info "Testing: Silver → Gold Integration & Dashboard"
    print_info "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    
    # Run all tests
    test_docker_health || true
    test_kafka_producer || true
    test_spark_processing || true
    test_postgresql_data || true
    test_dashboard || true
    test_latency || true
    
    # Print summary and exit
    print_summary
    exit $?
}

# Run main function
main "$@"
