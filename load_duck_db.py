import duckdb

conn = duckdb.connect('nyc_taxi.duckdb')

conn.execute("""
    CREATE SCHEMA IF NOT EXISTS raw
""")

conn.execute("""
    CREATE OR REPLACE TABLE raw.trips AS
    SELECT * FROM read_parquet('gcs/*/*/*.parquet')
""")

count = conn.execute("SELECT COUNT(*) FROM raw.trips").fetchone()[0]
print(f"Loaded {count} rows into raw.trips")

conn.close()
