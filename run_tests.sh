#!/bin/bash

################################################################################
# Crypto Pipeline - Test Execution Wrapper
# Simplifies running the complete automated test suite
################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_SCRIPT="$SCRIPT_DIR/automated_tests.py"
DOCKER_COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
ENV_FILE="$SCRIPT_DIR/.env"

# ============================================================================
# FUNCTIONS
# ============================================================================

print_header() {
    echo -e "\n${BOLD}${BLUE}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}$1${NC}"
    echo -e "${BOLD}${BLUE}══════════════════════════════════════════════════════════════${NC}\n"
}

log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

check_prerequisites() {
    print_header "🔍 Checking Prerequisites"
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 not found"
        exit 1
    fi
    log_success "Python 3 found: $(python3 --version)"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker not found"
        exit 1
    fi
    log_success "Docker found: $(docker --version)"
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose not found"
        exit 1
    fi
    log_success "Docker Compose found: $(docker-compose --version)"
    
    # Check PostgreSQL psql
    if ! command -v psql &> /dev/null; then
        log_warning "psql not found locally (will use Docker)"
    else
        log_success "psql found locally"
    fi
    
    # Check test script
    if [ ! -f "$TEST_SCRIPT" ]; then
        log_error "Test script not found: $TEST_SCRIPT"
        exit 1
    fi
    log_success "Test script found"
    
    # Check docker-compose file
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        log_error "docker-compose.yml not found"
        exit 1
    fi
    log_success "docker-compose.yml found"
}

check_services() {
    print_header "🚀 Checking Docker Services"
    
    log_info "Checking if Docker services are running..."
    
    # Get service status
    running=$(docker-compose ps --services --filter "status=running" 2>/dev/null | wc -l)
    total=$(docker-compose ps --services 2>/dev/null | wc -l)
    
    log_info "Running services: $running/$total"
    
    # Check critical services
    critical_services=("postgres" "kafka-0" "spark-master" "zookeeper")
    all_running=true
    
    for service in "${critical_services[@]}"; do
        if docker-compose ps "$service" 2>/dev/null | grep -q "running"; then
            log_success "Service $service is running"
        else
            log_warning "Service $service is not running"
            all_running=false
        fi
    done
    
    if [ "$all_running" = false ]; then
        log_warning "Some services are not running. Consider starting them:"
        echo -e "  ${YELLOW}cd $SCRIPT_DIR && docker-compose up -d${NC}"
    fi
}

install_dependencies() {
    print_header "📦 Installing Python Dependencies"
    
    # Check if psycopg2 is available
    if python3 -c "import psycopg2" 2>/dev/null; then
        log_success "psycopg2 already installed"
    else
        log_info "Installing psycopg2..."
        python3 -m pip install psycopg2-binary -q
        log_success "psycopg2 installed"
    fi
}

run_tests() {
    print_header "🧪 Running Automated Tests"
    
    log_info "Executing: python3 $TEST_SCRIPT"
    
    # Run the test script
    python3 "$TEST_SCRIPT"
    local exit_code=$?
    
    return $exit_code
}

show_usage() {
    cat << EOF
${BOLD}Crypto Pipeline - Test Suite${NC}

${BOLD}USAGE:${NC}
    ./run_tests.sh [OPTION]

${BOLD}OPTIONS:${NC}
    ${GREEN}(no args)${NC}       Run all tests (default)
    ${GREEN}--help${NC}           Show this help message
    ${GREEN}--quick${NC}          Quick health check only
    ${GREEN}--cleanup${NC}        Clean up test data after running
    ${GREEN}--verbose${NC}        Show detailed output
    ${GREEN}--no-prereq${NC}      Skip prerequisite checks
    ${GREEN}--no-services${NC}    Skip service health checks
    ${GREEN}--only-python${NC}    Run only Python tests (skip setup)

${BOLD}EXAMPLES:${NC}
    # Run all tests with prerequisites check
    ./run_tests.sh

    # Quick health check
    ./run_tests.sh --quick

    # Run tests and clean up
    ./run_tests.sh --cleanup

${BOLD}ENVIRONMENT VARIABLES:${NC}
    DB_HOST          PostgreSQL host (default: localhost)
    DB_PORT          PostgreSQL port (default: 5432)
    DB_USER          Database user (default: crypto_user)
    DB_PASS          Database password (default: crypto_password)
    DB_NAME          Database name (default: cryptopipeline)
    VERBOSE          Enable verbose output (default: 0)

${BOLD}REQUIREMENTS:${NC}
    - Python 3.6+
    - Docker
    - Docker Compose
    - PostgreSQL client (psql) - optional
    - psycopg2 Python package

${BOLD}OUTPUT:${NC}
    Test results are printed to stdout with color-coded status:
    ✅ PASSED  - Test succeeded
    ❌ FAILED  - Test failed (critical)
    ⚠️  WARNING - Test passed with warnings
    ⏭️  SKIPPED - Test was skipped

EOF
}

show_quick_check() {
    print_header "⚡ Quick Health Check"
    
    log_info "Checking critical components..."
    
    # Database connection
    if python3 -c "import psycopg2; psycopg2.connect(host='localhost', port=5432, database='cryptopipeline', user='crypto_user', password='crypto_password')" 2>/dev/null; then
        log_success "PostgreSQL is accessible"
    else
        log_error "PostgreSQL is not accessible"
        return 1
    fi
    
    # Docker services
    if docker-compose ps 2>/dev/null | grep -q "postgres.*running"; then
        log_success "PostgreSQL container is running"
    else
        log_error "PostgreSQL container is not running"
        return 1
    fi
    
    if docker-compose ps 2>/dev/null | grep -q "spark-master.*running"; then
        log_success "Spark is running"
    else
        log_warning "Spark is not running"
    fi
    
    log_success "Basic health check passed"
    return 0
}

cleanup() {
    print_header "🧹 Cleaning Up Test Data"
    
    log_info "Removing test records from database..."
    
    python3 << 'PYTHON_CLEANUP'
import psycopg2
import os

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME', 'cryptopipeline'),
        user=os.getenv('DB_USER', 'crypto_user'),
        password=os.getenv('DB_PASS', 'crypto_password')
    )
    cursor = conn.cursor()
    cursor.execute("DELETE FROM gold.gold_crypto_ohlcv WHERE symbol = 'TEST-USD';")
    deleted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Deleted {deleted} test records")
except Exception as e:
    print(f"❌ Cleanup failed: {e}")
PYTHON_CLEANUP
}

# ============================================================================
# MAIN
# ============================================================================

main() {
    local skip_prereq=false
    local skip_services=false
    local quick_check=false
    local cleanup_after=false
    local only_python=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help)
                show_usage
                exit 0
                ;;
            --quick)
                quick_check=true
                shift
                ;;
            --cleanup)
                cleanup_after=true
                shift
                ;;
            --verbose)
                export VERBOSE=1
                shift
                ;;
            --no-prereq)
                skip_prereq=true
                shift
                ;;
            --no-services)
                skip_services=true
                shift
                ;;
            --only-python)
                only_python=true
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Change to script directory
    cd "$SCRIPT_DIR"
    
    # Quick check mode
    if [ "$quick_check" = true ]; then
        show_quick_check
        exit $?
    fi
    
    # Only Python mode - skip all setup
    if [ "$only_python" = false ]; then
        # Prerequisites
        if [ "$skip_prereq" = false ]; then
            check_prerequisites
        fi
        
        # Service checks
        if [ "$skip_services" = false ]; then
            check_services
        fi
        
        # Install dependencies
        install_dependencies
    fi
    
    # Run tests
    run_tests
    local test_exit=$?
    
    # Cleanup if requested
    if [ "$cleanup_after" = true ]; then
        cleanup
    fi
    
    # Final summary
    if [ $test_exit -eq 0 ]; then
        print_header "✅ All Tests Completed Successfully"
        exit 0
    else
        print_header "❌ Tests Failed"
        exit 1
    fi
}

# Run main function
main "$@"
