# NYC Taxi Pipeline

A local data pipeline using Python, Pydantic, DuckDB, and dbt to mimic GCS -> BIGQUERY ingestion and transformations using dbt

---

## Project Structure

```
nyc_taxi_pipeline/
├── models.py                  # Pydantic schema
├── validate.py                # Row validation logic
├── ingest.py                  # Main ingestion script
├── load_duckdb.py             # Loads parquet into DuckDB
├── nyc_taxi.duckdb            # Local DuckDB database
├── gcs/                       # Partitioned parquet files
│       └── 2024/
│           └── 01/
└── dbt_project/               # dbt transformation models
    ├── profiles.yml
    ├── dbt_project.yml
    ├── packages.yml
    └── models/
        ├── bronze/
        │   ├── sources.yml
        │   └── stg_trips.sql
        ├── silver/
        │   └── trips_cleaned.sql
        └── gold/
            ├── dim_date.sql
            ├── dim_vendor.sql
            ├── dim_payment.sql
            ├── dim_location.sql
            └── fact_trips.sql
```

---

## Prerequisites

- Python 3.9+
- pip
- git

---

## Setup

### 1. Clone the repo

```bash
git clone <your-repo-url>
cd nyc_taxi_pipeline
```

### 2. Create and activate virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

> Every time you open a new terminal, run `source venv/bin/activate` first.

### 3. Install Python dependencies

```bash
pip install pandas pyarrow pydantic duckdb requests dbt-duckdb
```

### 4. Verify DuckDB is installed

```bash
python3 -c "import duckdb; print(duckdb.__version__)"
```

---

## Download Source Data

Download the NYC TLC Yellow Taxi data for January 2024:

```bash
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

---

## Run the Pipeline

### Step 1 — Ingest and validate

Reads the parquet file, validates each row with Pydantic, and saves clean partitioned parquet files to the `gcs/` folder.

```bash
python3 ingest.py
```

Expected output:
```
Valid rows:   96
Invalid rows: 4
Saved N rows → gcs/yellow/2024/01/2024-01-01.parquet
...
```

### Step 2 — Load into DuckDB

Reads all partitioned parquet files from `gcs/` and loads them into the `raw.trips` table in DuckDB.

```bash
python3 load_duckdb.py
```

Expected output:
```
Loaded 96 rows into raw.trips
```

### Step 3 — Run dbt models

Navigate into the dbt project and install dbt packages first:

```bash
cd dbt_project
dbt deps --profiles-dir .
```

Then run all models:

```bash
dbt run --profiles-dir .
```

Expected output:
```
OK  bronze.stg_trips
OK  silver.trips_cleaned
OK  gold.dim_date
OK  gold.dim_vendor
OK  gold.dim_payment
OK  gold.dim_location
OK  gold.fact_trips
```

---

## Query the Data

Open Python from the project root and query any layer:

```bash
python3 -c "
import duckdb
conn = duckdb.connect('nyc_taxi.duckdb')
print(conn.execute('SELECT COUNT(*) FROM main_bronze.stg_trips').fetchone())
print(conn.execute('SELECT COUNT(*) FROM main_silver.trips_cleaned').fetchone())
print(conn.execute('SELECT COUNT(*) FROM main_gold.fact_trips').fetchone())
"
```

---

## Validation Rules (Pydantic)

Rows are rejected if they fail any of these checks:

| Field | Rule |
|---|---|
| `passenger_count` | Must be between 1 and 9 |
| `trip_distance` | Must be greater than 0 |
| `fare_amount` | Must be >= 0 |
| `tip_amount` | Must be >= 0 |
| `tolls_amount` | Must be >= 0 |
| `dropoff_at` | Must be after `pickup_at` |

Rejected rows are collected in `invalid_rows` with their error messages for investigation.

---

## Data Layers

| Layer | Schema | Type | Purpose |
|---|---|---|---|
| Raw | `raw` | Table | Immutable source data |
| Bronze | `main_bronze` | View | Cast types, rename columns |
| Silver | `main_silver` | View | Filter bad rows, add derived columns |
| Gold | `main_gold` | Table | Star schema, BI-ready |

---

## Gold Star Schema

```
dim_date ──────┐
dim_vendor ────┤
dim_location ──┼──→ fact_trips
dim_payment ───┘
```
