#!/usr/bin/env python3
"""
DUCKDB LAKEHOUSE E2E VERIFICATION SCRIPT
========================================
Simulates the Streamlit backend to verify data flows from MinIO to DuckDB.
SLA: Pipeline latency must be UNDER 15 seconds.
"""
import sys
from datetime import datetime, timezone

# Configuration
MINIO_ENDPOINT = "storage:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET = "warehouse"
SLA_THRESHOLD_SECONDS = 15

print("=" * 60)
print("  DUCKDB LAKEHOUSE E2E VERIFICATION")
print("  SLA Threshold: < 15 seconds")
print("=" * 60)

# Import dependencies
try:
    import boto3
    from botocore.config import Config
except ImportError as e:
    print(f"[FATAL] boto3 not installed: {e}")
    sys.exit(1)

try:
    import duckdb
except ImportError as e:
    print(f"[FATAL] duckdb not installed: {e}")
    sys.exit(1)

# Main execution
try:
    # ============================================================
    # STEP A: Boto3 Metadata Resolution
    # ============================================================
    print("\n[STEP A] Boto3: Resolving latest Iceberg metadata...")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    # Find the crypto_ohlcv table folder
    response = s3.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix="gold/",
        Delimiter="/",
    )

    table_prefix = None
    for obj in response.get("CommonPrefixes", []):
        prefix = obj["Prefix"]
        if "crypto_ohlcv" in prefix:
            table_prefix = prefix
            break

    if not table_prefix:
        print("[FAIL] Could not find crypto_ohlcv table in MinIO")
        sys.exit(1)

    print(f"  ✓ Found table: {table_prefix}")

    # List metadata files
    metadata_prefix = f"{table_prefix}metadata/"
    response = s3.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix=metadata_prefix,
    )

    metadata_files = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].endswith(".metadata.json")
    ]

    if not metadata_files:
        print("[FAIL] No metadata files found")
        sys.exit(1)

    # Get the newest metadata file
    metadata_files.sort(key=lambda x: x["LastModified"], reverse=True)
    latest_obj = metadata_files[0]
    latest_key = latest_obj["Key"]
    latest_metadata_uri = f"s3://{MINIO_BUCKET}/{latest_key}"
    latest_modified = latest_obj["LastModified"]

    print(f"  ✓ Latest metadata: {latest_key}")
    print(f"  ✓ Last modified: {latest_modified}")

    # ============================================================
    # STEP B: DuckDB Connection & Config
    # ============================================================
    print("\n[STEP B] DuckDB: Connecting to MinIO...")

    conn = duckdb.connect(database=":memory:")
    print("  ✓ In-memory database created")

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    print("  ✓ httpfs extension loaded")

    conn.execute("INSTALL iceberg; LOAD iceberg;")
    print("  ✓ iceberg extension loaded")

    # Configure S3
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_region='us-east-1';")
    print("  ✓ S3 configured for MinIO")

    # ============================================================
    # STEP C: Query Execution
    # ============================================================
    print("\n[STEP C] DuckDB: Querying Iceberg table...")

    # Get max timestamp
    query_max_ts = f"SELECT MAX(window_start) AS max_ts, COUNT(*) AS row_count FROM iceberg_scan('{latest_metadata_uri}')"
    result = conn.execute(query_max_ts).fetchone()
    max_ts_str = result[0]
    row_count = result[1]

    print(f"  ✓ Max timestamp: {max_ts_str}")
    print(f"  ✓ Total rows: {row_count}")

    # Get sample data
    query_sample = f"""
        SELECT symbol, window_start, window_end, open, high, low, close, volume
        FROM iceberg_scan('{latest_metadata_uri}')
        ORDER BY window_start DESC
        LIMIT 3
    """
    df_sample = conn.execute(query_sample).df()
    print(f"\n  Latest 3 candles:")
    for idx, row in df_sample.iterrows():
        print(f"    {row['symbol']} | {row['window_start']} | O:{row['open']:.2f} H:{row['high']:.2f} L:{row['low']:.2f} C:{row['close']:.2f}")

    conn.close()

    # ============================================================
    # STEP D: SLA VERIFICATION
    # ============================================================
    print("\n[STEP D] SLA Verification: Calculating latency...")

    # Parse the max timestamp
    if isinstance(max_ts_str, str):
        # Handle various timestamp formats
        max_ts_str_clean = max_ts_str.replace('Z', '+00:00')
        max_ts = datetime.fromisoformat(max_ts_str_clean)
    else:
        max_ts = max_ts_str

    # Current UTC time
    current_utc = datetime.now(timezone.utc)

    # Calculate lag
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)

    lag_seconds = (current_utc - max_ts).total_seconds()

    print(f"  Current UTC: {current_utc.isoformat()}")
    print(f"  Max candle: {max_ts.isoformat()}")
    print(f"  Pipeline lag: {lag_seconds:.1f} seconds")

    # SLA Check
    print(f"\n{'=' * 60}")
    if lag_seconds < SLA_THRESHOLD_SECONDS:
        print(f"  ✅ SLA ASSERTION PASSED")
        print(f"  Latency: {lag_seconds:.1f}s < {SLA_THRESHOLD_SECONDS}s threshold")
        print(f"  Status: ALL GREEN ✅")
        print(f"{'=' * 60}")
        sys.exit(0)
    else:
        print(f"  ❌ SLA ASSERTION FAILED")
        print(f"  Latency: {lag_seconds:.1f}s >= {SLA_THRESHOLD_SECONDS}s threshold")
        print(f"  Status: DEGRADED ❌")
        print(f"{'=' * 60}")
        sys.exit(1)

except Exception as e:
    print(f"\n[FATAL ERROR] {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
