# Big Data Project — Taxi ETL & Analytics

A simple ETL pipeline and analytics playground for a taxi trips dataset that demonstrates a bronze → silver → gold processing pattern and stores results in Apache Cassandra (Astra DB compatible). Includes ingestion, cleaning/curation, aggregation jobs, and a few visualization scripts.

See the short demo: https://youtu.be/cZVOHB8HRfc

## Stack
- Language(s): Python (primary), CQL for schema
- Runtime / services: Python 3.10+ with Cassandra (DataStax / Astra DB)
- Notable libraries: cassandra-driver, pandas, numpy, matplotlib / seaborn

## What this repo contains (top-level)
```
.env.sample            # example environment variables
all_schema.cql         # CQL schema for keyspace / tables
bronze.py              # ingest CSV -> bronze table (raw/parsed rows)
silver.py              # transform/clean bronze -> silver (curated)
gold_area_revenue.py   # aggregate revenue by area & date -> gold_area_1
gold_duration.py       # classify trip durations and write summary -> gold_duration
gold_payment.py        # aggregate metrics by payment_type -> gold_payment
connect_db.py          # Cassandra/Astra connection helper (reads env)
requirements.txt       # Python deps
plots/                 # output folder for charts (created/used by viz scripts)
taxi.csv               # full dataset (large)
viz_area_rev.py        # visualization of area revenue
viz_time_category.py   # visualization of time-category buckets
README.md              # this file
```

## How it fits together
The repo implements a simple ETL flow: bronze.py ingests raw CSV rows into a bronze table in Cassandra (parsed but minimally cleaned), silver.py reads bronze and writes a curated silver table with normalized date/time and validated numeric fields, and the gold_* scripts compute aggregates and summaries from silver and write them into gold tables. Visualizations load aggregated gold tables and produce plots into the plots/ directory.

## Quickstart — from clone to running
1. Create a Python virtualenv and install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Provide Cassandra/Astra credentials:
- Copy `.env.sample` -> `.env` and set:
  - `ASTRA_DB_APPLICATION_TOKEN` — your Astra DB token (or other password for PlainTextAuthProvider)
  - `SECURE_CONNECT_BUNDLE` — path to the Astra secure connect bundle zip (or your cloud bundle)
- connect_db.py sets the keyspace to `taxi` by default. If you use a different keyspace, update it there or export another env var and adapt the file.

Example `.env`:
```env
ASTRA_DB_APPLICATION_TOKEN=your_astra_token_here
SECURE_CONNECT_BUNDLE=/path/to/secure-connect-database.zip
```

3. Create the keyspace / tables
- Use the provided CQL file:
```bash
# using cqlsh / dse or DataStax Astra cql shell tooling
cqlsh> SOURCE 'all_schema.cql';
```
(If using Astra DB, use the GUI or cqlsh with the secure connect bundle.)

4. Prepare the dataset
- The repo contains `taxi.csv` (large). If you can't or don't want to push the whole file, substitute a smaller CSV with the same columns or generate a sample subset.

5. Run the pipeline (example)
- Ingest raw CSV to bronze:
```bash
python bronze.py
```
- Build curated silver table from bronze:
```bash
python silver.py
```
- Build gold-level aggregates:
```bash
python gold_area_revenue.py
python gold_duration.py
python gold_payment.py
```
- Generate visualizations (these write to the `plots/` directory):
```bash
python viz_area_rev.py
python viz_time_category.py
```

## Scripts — what they do
- bronze.py
  - Reads `taxi.csv`, parses timestamps and numeric fields, and inserts rows into a `bronze` table in Cassandra in batches.
- silver.py
  - Reads the `bronze` table, filters invalid/empty rows, normalizes fields (dates/times), and writes to `silver`.
- gold_area_revenue.py
  - For each date and pickup community area, aggregates totals and averages (fare, tips, trip_total) and writes results to `gold_area_1`.
- gold_duration.py
  - Classifies trips into duration buckets (`short`, `medium`, `long`) and stores counts in `gold_duration`.
- gold_payment.py
  - Aggregates metrics by `payment_type` and writes to `gold_payment`.
- viz_*.py
  - Read aggregated results and create plots (matplotlib/seaborn) saved into `plots/`.

## Data & schema
- The CQL schema is provided in `all_schema.cql`. It defines the `taxi` keyspace and tables used by the scripts (bronze, silver, gold_*).
- Cassandra connectivity: connect_db.py uses the DataStax PlainTextAuthProvider and a cloud secure bundle. The code sets keyspace to `taxi` by default.

