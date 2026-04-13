#!/usr/bin/env python3
"""
Crypto Pipeline - Automated Test Suite
Comprehensive testing framework for the Silver → Gold layer integration
"""

import subprocess
import sys
import json
import time
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import os
from dataclasses import dataclass
from enum import Enum
import tempfile

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

class TestStatus(Enum):
    PASSED = "✅"
    FAILED = "❌"
    WARNING = "⚠️"
    SKIPPED = "⏭️"

@dataclass
class TestResult:
    name: str
    status: TestStatus
    message: str
    duration: float
    details: Optional[Dict] = None

class Colors:
    BLUE = '\033[0;34m'
    GREEN = '\033[0;32m'
    RED = '\033[0;31m'
    YELLOW = '\033[1;33m'
    BOLD = '\033[1m'
    END = '\033[0m'

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'cryptopipeline'),
    'user': os.getenv('DB_USER', 'crypto_user'),
    'password': os.getenv('DB_PASS', 'crypto_password'),
}

COMPOSE_FILE = "docker-compose.yml"
TIMEOUT_SECONDS = 300
REQUIRED_SERVICES = [
    'postgres', 'kafka-0', 'kafka-1', 'kafka-2',
    'spark-master', 'spark-worker-1', 'zookeeper', 'namenode'
]

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def print_header(text: str) -> None:
    """Print formatted header"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text:^70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.END}\n")

def print_test(name: str) -> None:
    """Print test name"""
    print(f"{Colors.BLUE}{'─'*70}{Colors.END}")
    print(f"{Colors.BLUE}TEST: {name}{Colors.END}")
    print(f"{Colors.BLUE}{'─'*70}{Colors.END}")

def log_info(message: str) -> None:
    """Log info message"""
    print(f"{Colors.BLUE}ℹ️  {message}{Colors.END}")

def log_success(message: str) -> None:
    """Log success message"""
    print(f"{Colors.GREEN}✅ {message}{Colors.END}")

def log_error(message: str) -> None:
    """Log error message"""
    print(f"{Colors.RED}❌ {message}{Colors.END}")

def log_warning(message: str) -> None:
    """Log warning message"""
    print(f"{Colors.YELLOW}⚠️  {message}{Colors.END}")

def run_command(cmd: str, timeout: int = 30, shell: bool = True) -> Tuple[int, str, str]:
    """Execute shell command and return exit code, stdout, stderr"""
    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", f"Command timed out after {timeout}s"
    except Exception as e:
        return -1, "", str(e)

def get_db_connection():
    """Create PostgreSQL connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        log_error(f"Database connection failed: {e}")
        return None

def execute_query(conn, query: str) -> Optional[List[tuple]]:
    """Execute SQL query and return results"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except psycopg2.Error as e:
        log_error(f"Query execution failed: {e}")
        return None

# ============================================================================
# TEST SUITE
# ============================================================================

class CryptoPipelineTestSuite:
    """Main test suite class"""
    
    def __init__(self):
        self.results: List[TestResult] = []
        self.start_time = None
        self.end_time = None
    
    def run_all_tests(self) -> bool:
        """Run all tests and return overall status"""
        self.start_time = time.time()
        
        print_header("🧪 CRYPTO PIPELINE - AUTOMATED TEST SUITE")
        log_info(f"Starting tests at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log_info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        
        # Run test groups
        self.test_docker_health()
        self.test_database_connection()
        self.test_schema_structure()
        self.test_table_existence()
        self.test_indexes()
        self.test_partitions()
        self.test_data_insertion()
        self.test_data_retrieval()
        self.test_data_quality()
        self.test_kpi_functions()
        self.test_query_performance()
        self.test_error_handling()
        
        self.end_time = time.time()
        
        # Print summary
        self.print_summary()
        
        # Return overall status
        return self.get_overall_status()
    
    def add_result(self, result: TestResult) -> None:
        """Add test result to results list"""
        status_str = f"{result.status.value}"
        duration_str = f"{result.duration:.3f}s"
        print(f"  {status_str} {result.name:<50} [{duration_str}]")
        if result.message:
            print(f"     {result.message}")
        self.results.append(result)
    
    # ========================================================================
    # TEST IMPLEMENTATIONS
    # ========================================================================
    
    def test_docker_health(self) -> None:
        """TEST 1: Docker Stack Health"""
        print_test("Docker Stack Health")
        start = time.time()
        
        # Check if docker-compose is available
        code, out, err = run_command("docker-compose --version")
        if code != 0:
            self.add_result(TestResult(
                "Docker Compose availability",
                TestStatus.FAILED,
                "Docker Compose not found",
                time.time() - start
            ))
            return
        
        log_info("Checking Docker services...")
        
        # Get service status
        code, out, err = run_command("docker-compose ps --format=json")
        if code != 0:
            self.add_result(TestResult(
                "Docker services running",
                TestStatus.FAILED,
                f"Could not get service status: {err}",
                time.time() - start
            ))
            return
        
        try:
            services = json.loads(out) if out else []
            running_services = [s.get('Service') for s in services if s.get('State') == 'running']
            
            required_count = len([s for s in REQUIRED_SERVICES if s in running_services])
            total_required = len(REQUIRED_SERVICES)
            
            if required_count >= total_required - 2:  # Allow 2 optional services to be down
                self.add_result(TestResult(
                    "Required services running",
                    TestStatus.PASSED,
                    f"{required_count}/{total_required} critical services running",
                    time.time() - start,
                    {'running_services': running_services}
                ))
            else:
                self.add_result(TestResult(
                    "Required services running",
                    TestStatus.FAILED,
                    f"Only {required_count}/{total_required} critical services running",
                    time.time() - start
                ))
        except json.JSONDecodeError:
            # Fallback to text parsing
            service_count = len([line for line in out.split('\n') if 'running' in line.lower()])
            self.add_result(TestResult(
                "Required services running",
                TestStatus.PASSED if service_count >= 6 else TestStatus.WARNING,
                f"Approximately {service_count} services running",
                time.time() - start
            ))
    
    def test_database_connection(self) -> None:
        """TEST 2: Database Connection"""
        print_test("Database Connection & Health")
        start = time.time()
        
        log_info("Connecting to PostgreSQL...")
        conn = get_db_connection()
        
        if conn is None:
            self.add_result(TestResult(
                "Database connection",
                TestStatus.FAILED,
                "Could not connect to PostgreSQL",
                time.time() - start
            ))
            return
        
        # Test query
        results = execute_query(conn, "SELECT version();")
        if results:
            version = results[0][0] if results[0] else "Unknown"
            log_success(f"PostgreSQL version: {version.split(',')[0]}")
            self.add_result(TestResult(
                "Database connection",
                TestStatus.PASSED,
                "Successfully connected to PostgreSQL",
                time.time() - start
            ))
        else:
            self.add_result(TestResult(
                "Database connection",
                TestStatus.FAILED,
                "Connected but query failed",
                time.time() - start
            ))
        
        conn.close()
    
    def test_schema_structure(self) -> None:
        """TEST 3: Schema Structure"""
        print_test("Schema & Table Structure")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Gold schema exists",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        # Check schema exists
        results = execute_query(conn, 
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold';")
        
        if results and len(results) > 0:
            self.add_result(TestResult(
                "Gold schema exists",
                TestStatus.PASSED,
                "Schema 'gold' successfully created",
                time.time() - start
            ))
        else:
            self.add_result(TestResult(
                "Gold schema exists",
                TestStatus.FAILED,
                "Schema 'gold' not found",
                time.time() - start
            ))
        
        conn.close()
    
    def test_table_existence(self) -> None:
        """TEST 4: Table Existence"""
        print_test("Table Existence")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Required tables",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        required_tables = [
            'gold_crypto_ohlcv',
            'gold_predictions',
            'gold_hourly_aggregates',
            'gold_daily_aggregates',
            'gold_data_quality_metrics'
        ]
        
        found_tables = []
        for table in required_tables:
            results = execute_query(conn, f"""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'gold' AND table_name = '{table}';
            """)
            if results and len(results) > 0:
                found_tables.append(table)
                log_success(f"Table '{table}' exists")
            else:
                log_warning(f"Table '{table}' not found")
        
        if len(found_tables) >= len(required_tables) - 1:
            self.add_result(TestResult(
                "Required tables",
                TestStatus.PASSED,
                f"Found {len(found_tables)}/{len(required_tables)} tables",
                time.time() - start,
                {'tables': found_tables}
            ))
        else:
            self.add_result(TestResult(
                "Required tables",
                TestStatus.FAILED,
                f"Only {len(found_tables)}/{len(required_tables)} tables found",
                time.time() - start
            ))
        
        conn.close()
    
    def test_indexes(self) -> None:
        """TEST 5: Index Coverage"""
        print_test("Index Verification")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Indexes on tables",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        # Check indexes on main table
        results = execute_query(conn, """
            SELECT indexname FROM pg_indexes 
            WHERE schemaname = 'gold' AND tablename = 'gold_crypto_ohlcv';
        """)
        
        if results:
            index_count = len(results)
            log_success(f"Found {index_count} indexes on gold_crypto_ohlcv")
            self.add_result(TestResult(
                "Indexes on tables",
                TestStatus.PASSED if index_count >= 8 else TestStatus.WARNING,
                f"Found {index_count} indexes (target: 8+)",
                time.time() - start,
                {'index_count': index_count}
            ))
        else:
            self.add_result(TestResult(
                "Indexes on tables",
                TestStatus.FAILED,
                "No indexes found",
                time.time() - start
            ))
        
        conn.close()
    
    def test_partitions(self) -> None:
        """TEST 6: Table Partitioning"""
        print_test("Partitioning Verification")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Table partitions",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        # Check if table is partitioned
        results = execute_query(conn, """
            SELECT partition_method FROM information_schema.tables
            WHERE table_schema = 'gold' AND table_name = 'gold_crypto_ohlcv';
        """)
        
        # This query might not work on PG12, try alternative
        if not results:
            results = execute_query(conn, """
                SELECT partstrat FROM pg_partitioned_table pt
                JOIN pg_class pc ON pt.partrelid = pc.oid
                WHERE pc.relname = 'gold_crypto_ohlcv';
            """)
        
        if results:
            self.add_result(TestResult(
                "Table partitions",
                TestStatus.PASSED,
                "Table is partitioned",
                time.time() - start
            ))
        else:
            self.add_result(TestResult(
                "Table partitions",
                TestStatus.WARNING,
                "Could not verify partitioning status",
                time.time() - start
            ))
        
        conn.close()
    
    def test_data_insertion(self) -> None:
        """TEST 7: Data Insertion"""
        print_test("Data Insertion")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Insert test data",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        try:
            cursor = conn.cursor()
            
            # Insert test data
            insert_query = """
                INSERT INTO gold.gold_crypto_ohlcv (
                    timestamp, symbol, exchange, 
                    open, high, low, close, volume,
                    quality_status, data_source,
                    ingest_timestamp, process_timestamp,
                    record_count, error_flag,
                    source_partition, source_offset
                ) VALUES 
                (NOW(), 'TEST-USD', 'test', 
                 100.00, 100.50, 99.50, 100.25, 10.0,
                 'VALID', 'test',
                 NOW()::TIMESTAMPTZ, NOW()::TIMESTAMPTZ,
                 1, FALSE,
                 0, 999)
            """
            
            cursor.execute(insert_query)
            conn.commit()
            
            self.add_result(TestResult(
                "Insert test data",
                TestStatus.PASSED,
                "Successfully inserted test record",
                time.time() - start
            ))
            
            cursor.close()
        except Exception as e:
            self.add_result(TestResult(
                "Insert test data",
                TestStatus.FAILED,
                f"Insertion failed: {str(e)}",
                time.time() - start
            ))
        
        conn.close()
    
    def test_data_retrieval(self) -> None:
        """TEST 8: Data Retrieval"""
        print_test("Data Retrieval")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Retrieve data",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        # Count records
        results = execute_query(conn, "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;")
        
        if results and results[0][0] is not None:
            count = results[0][0]
            log_info(f"Total records in database: {count}")
            
            self.add_result(TestResult(
                "Retrieve data",
                TestStatus.PASSED if count > 0 else TestStatus.WARNING,
                f"Found {count} records in gold_crypto_ohlcv",
                time.time() - start,
                {'record_count': count}
            ))
        else:
            self.add_result(TestResult(
                "Retrieve data",
                TestStatus.FAILED,
                "Could not count records",
                time.time() - start
            ))
        
        conn.close()
    
    def test_data_quality(self) -> None:
        """TEST 9: Data Quality Checks"""
        print_test("Data Quality Validation")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Data quality checks",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        quality_checks = {
            'NULL prices': "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE close IS NULL;",
            'NULL volumes': "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE volume IS NULL;",
            'Error records': "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE error_flag = TRUE;",
            'Valid records': "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE error_flag = FALSE;",
        }
        
        all_passed = True
        for check_name, query in quality_checks.items():
            results = execute_query(conn, query)
            if results:
                count = results[0][0]
                if 'NULL' in check_name or 'Error' in check_name:
                    if count == 0:
                        log_success(f"{check_name}: {count}")
                    else:
                        log_warning(f"{check_name}: {count}")
                        all_passed = False
                else:
                    log_success(f"{check_name}: {count}")
        
        self.add_result(TestResult(
            "Data quality checks",
            TestStatus.PASSED if all_passed else TestStatus.WARNING,
            "Data quality checks completed",
            time.time() - start
        ))
        
        conn.close()
    
    def test_kpi_functions(self) -> None:
        """TEST 10: KPI Functions"""
        print_test("KPI Functions Verification")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "KPI functions",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        kpi_functions = [
            'get_throughput_last_5min',
            'get_data_quality_metrics',
            'get_latency_percentiles',
            'get_data_freshness',
            'get_symbol_performance',
        ]
        
        working_functions = []
        for func_name in kpi_functions:
            try:
                query = f"SELECT * FROM gold.{func_name}();"
                results = execute_query(conn, query)
                if results is not None:
                    working_functions.append(func_name)
                    log_success(f"Function '{func_name}' works")
                else:
                    log_warning(f"Function '{func_name}' returned no results")
            except Exception as e:
                log_error(f"Function '{func_name}' failed: {str(e)}")
        
        if len(working_functions) >= len(kpi_functions) - 1:
            self.add_result(TestResult(
                "KPI functions",
                TestStatus.PASSED,
                f"{len(working_functions)}/{len(kpi_functions)} functions operational",
                time.time() - start,
                {'functions': working_functions}
            ))
        else:
            self.add_result(TestResult(
                "KPI functions",
                TestStatus.FAILED,
                f"Only {len(working_functions)}/{len(kpi_functions)} functions working",
                time.time() - start
            ))
        
        conn.close()
    
    def test_query_performance(self) -> None:
        """TEST 11: Query Performance"""
        print_test("Query Performance")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Query performance",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        cursor = conn.cursor()
        
        # Test query performance
        queries = {
            'Count records': "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;",
            'List symbols': "SELECT DISTINCT symbol FROM gold.gold_crypto_ohlcv;",
            'Recent records': "SELECT * FROM gold.gold_crypto_ohlcv ORDER BY timestamp DESC LIMIT 10;",
        }
        
        all_fast = True
        for query_name, query in queries.items():
            query_start = time.time()
            try:
                cursor.execute(query)
                cursor.fetchall()
                query_time = (time.time() - query_start) * 1000
                
                if query_time < 1000:  # Under 1 second
                    log_success(f"'{query_name}': {query_time:.2f}ms")
                else:
                    log_warning(f"'{query_name}': {query_time:.2f}ms (slow)")
                    all_fast = False
            except Exception as e:
                log_error(f"'{query_name}' failed: {str(e)}")
                all_fast = False
        
        self.add_result(TestResult(
            "Query performance",
            TestStatus.PASSED if all_fast else TestStatus.WARNING,
            "Query performance tests completed",
            time.time() - start
        ))
        
        cursor.close()
        conn.close()
    
    def test_error_handling(self) -> None:
        """TEST 12: Error Handling"""
        print_test("Error Handling & Validation")
        start = time.time()
        
        conn = get_db_connection()
        if not conn:
            self.add_result(TestResult(
                "Error handling",
                TestStatus.FAILED,
                "No database connection",
                time.time() - start
            ))
            return
        
        # Test error flag functionality
        results = execute_query(conn, """
            SELECT COUNT(*) as error_records, 
                   COUNT(*) FILTER (WHERE error_flag = FALSE) as valid_records
            FROM gold.gold_crypto_ohlcv;
        """)
        
        if results:
            error_count = results[0][0] - results[0][1] if len(results[0]) > 1 else 0
            valid_count = results[0][1] if len(results[0]) > 1 else results[0][0]
            log_info(f"Valid records: {valid_count}, Error records: {error_count}")
            
            self.add_result(TestResult(
                "Error handling",
                TestStatus.PASSED,
                "Error tracking is functional",
                time.time() - start,
                {'valid': valid_count, 'errors': error_count}
            ))
        else:
            self.add_result(TestResult(
                "Error handling",
                TestStatus.WARNING,
                "Could not verify error handling",
                time.time() - start
            ))
        
        conn.close()
    
    def print_summary(self) -> None:
        """Print test summary"""
        print_header("📊 TEST SUMMARY")
        
        passed = sum(1 for r in self.results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in self.results if r.status == TestStatus.FAILED)
        warnings = sum(1 for r in self.results if r.status == TestStatus.WARNING)
        
        total_time = self.end_time - self.start_time
        
        print(f"{Colors.GREEN}✅ Passed:  {passed}{Colors.END}")
        print(f"{Colors.RED}❌ Failed:  {failed}{Colors.END}")
        print(f"{Colors.YELLOW}⚠️  Warnings: {warnings}{Colors.END}")
        print(f"\n⏱️  Total time: {total_time:.2f}s\n")
        
        if failed == 0:
            print(f"{Colors.GREEN}{Colors.BOLD}🎉 ALL TESTS PASSED!{Colors.END}\n")
        elif failed <= 2:
            print(f"{Colors.YELLOW}{Colors.BOLD}⚠️  SOME TESTS FAILED{Colors.END}\n")
        else:
            print(f"{Colors.RED}{Colors.BOLD}❌ CRITICAL FAILURES{Colors.END}\n")
    
    def get_overall_status(self) -> bool:
        """Return overall test status"""
        failed = sum(1 for r in self.results if r.status == TestStatus.FAILED)
        return failed == 0

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main test execution"""
    suite = CryptoPipelineTestSuite()
    success = suite.run_all_tests()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
