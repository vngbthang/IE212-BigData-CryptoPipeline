from trino.dbapi import connect

conn = connect(
    host="localhost", port=8080,
    user="airflow", catalog="nessie",
    schema="gold", http_scheme="http"
)
cur = conn.cursor()
try:
    cur.execute(
        'CREATE SCHEMA IF NOT EXISTS nessie.gold '
        'WITH (location = \'s3a://crypto-lake/warehouse/gold\')'
    )
    conn.commit()
    print("Schema nessie.gold created/verified OK")
except Exception as e:
    print(f"Schema create: {e}")

# Try SHOW SCHEMAS
try:
    cur.execute("SHOW SCHEMAS FROM nessie")
    rows = cur.fetchall()
    print(f"Schemas in nessie: {[r[0] for r in rows]}")
except Exception as e:
    print(f"SHOW SCHEMAS failed: {e}")

conn.close()
