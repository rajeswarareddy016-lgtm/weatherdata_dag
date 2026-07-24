# Weather Hourly Data Pipeline

An hourly ETL pipeline that ingests live weather data from a public REST API,
validates and enriches it, and loads it into Snowflake — orchestrated by
Apache Airflow. DAG id: **`weather_hourly_data_pipeline`**.

---

## 1. Approach

The pipeline follows a classic **Extract → Transform → Load** design, built as an
Airflow DAG with three independent, retryable tasks.

```
Open-Meteo API  ──►  extract_data  ──►  transform_data  ──►  load_data  ──►  Snowflake
   (REST)            (get JSON)        (validate + tag)      (insert)     CITY_WEATHER_METRICS
```

| Task | Responsibility |
|------|----------------|
| **extract_data**   | Calls the Open-Meteo `current` weather endpoint and pushes the raw JSON to XCom. Raises on any HTTP/network error so Airflow retries. |
| **transform_data** | Validates the payload (`current` present, `temperature_2m` present), flattens it into one row containing **all** weather fields + location metadata, and **adds two metadata columns**: `SOURCE_NAME` and `LOAD_TIMESTAMP`. |
| **load_data**      | Connects to Snowflake via the Airflow `SnowflakeHook`, creates the table if missing, and bulk-inserts the row(s). |

**Why one row per run:** the pipeline uses the API's `current` endpoint, which
returns a single real-time snapshot. Since the DAG runs **hourly**, it appends one
complete weather reading each hour — a true hourly time series.

**Design choices (Airflow best practices):**
- **Modular tasks** — extract / transform / load are separate `PythonOperator`
  tasks, wired `extract_data >> transform_data >> load_data`.
- **Retries** — every task retries **3 times** with a 2-minute delay
  (`default_args`), to survive transient API/network failures.
- **Logging** — pipeline start/end and per-stage record counts are logged.
- **Idempotent scheduling** — `catchup=False`, `max_active_runs=1`.

---

## 2. Technologies Used

- **Python 3.12**
- **Apache Airflow 3.3.0** (orchestration & scheduling)
- **apache-airflow-providers-snowflake** (`SnowflakeHook`)
- **requests** (REST client)
- **Snowflake** (destination data warehouse)
- **Open-Meteo API** (free public weather REST API — no key required)
- Runs on **WSL2 / Ubuntu** (Airflow is not supported natively on Windows)

---

## 3. Files

| File | Purpose |
|------|---------|
| `hourly_weather_pipeline.py` | The Airflow DAG (extract, transform, load) |
| `create_table.sql`           | DDL for the Snowflake destination table |
| `README.md`                  | This document |

---

## 4. Destination Table

`WEATHER_DB.RAW.CITY_WEATHER_METRICS` — location metadata + all current-weather
fields + the two metadata columns (`SOURCE_NAME`, `LOAD_TIMESTAMP`). Full DDL is
in **`create_table.sql`**. Create it once:

```sql
-- in Snowsight, run the contents of create_table.sql
```

---

## 5. How to Execute the Pipeline

> Airflow runs in WSL2 (Ubuntu). These are the exact steps used for this project.

### Step 1 — Create the Snowflake table
Run `create_table.sql` in a Snowsight worksheet.

### Step 2 — Install Airflow + the Snowflake provider (one time)
```bash
python3.12 -m venv ~/airflow_env
source ~/airflow_env/bin/activate
pip install "apache-airflow==3.3.0"
pip install apache-airflow-providers-snowflake requests
```

### Step 3 — Deploy the DAG
```bash
mkdir -p ~/airflow/dags
cp hourly_weather_pipeline.py ~/airflow/dags/
```

### Step 4 — Start Airflow
```bash
airflow standalone      # starts scheduler + api-server; prints an admin password
```
Open the UI at **http://localhost:8080** (user `admin`).

### Step 5 — Add the Snowflake connection
In the UI: **Admin → Connections → +**

| Field | Value |
|-------|-------|
| Connection Id | `snowflake_default` |
| Connection Type | `Snowflake` |
| Account | `FVDTPUC-JKB20241` |
| Login | `Rajesh5109` |
| Password | *your Snowflake password* |
| Warehouse | `COMPUTE_WH` |
| Database | `WEATHER_DB` |
| Schema | `RAW` |
| Role | `ACCOUNTADMIN` |

Or via CLI:
```bash
airflow connections add snowflake_default \
  --conn-type snowflake \
  --conn-login 'Rajesh5109' \
  --conn-password '********' \
  --conn-schema 'RAW' \
  --conn-extra '{"account":"FVDTPUC-JKB20241","warehouse":"COMPUTE_WH","database":"WEATHER_DB","role":"ACCOUNTADMIN"}'
```

### Step 6 — Enable and run
```bash
airflow dags unpause weather_hourly_data_pipeline
airflow dags trigger weather_hourly_data_pipeline    # run immediately (optional)
```
The DAG now runs automatically **every hour**.

### Verify the load in Snowflake
```sql
SELECT COUNT(*) AS row_count, MAX(LOAD_TIMESTAMP) AS last_loaded
FROM WEATHER_DB.RAW.CITY_WEATHER_METRICS;

SELECT * FROM WEATHER_DB.RAW.CITY_WEATHER_METRICS
ORDER BY LOAD_TIMESTAMP DESC LIMIT 10;
```

---

## 6. Logging, Retries & Error Handling

- **Logging** — each task logs its start and the number of records processed; view
  in the UI via **Grid → task → Logs**.
- **Retries** — `retries=3`, `retry_delay=2 min` for every task (`default_args`).
- **Error handling**
  - *extract*: raises on non-2xx HTTP / network errors → task retries.
  - *transform*: raises `ValueError`/`KeyError` if the JSON is malformed or the
    core `temperature_2m` field is missing.
  - *load*: wraps the insert in `try/except/finally`; rolls back on failure and
    always closes the Snowflake connection.

---

## 7. Screenshots (successful DAG run)

Include the following screenshots from the Airflow UI (save them in a
`screenshots/` folder next to this README and reference them below):

1. **DAGs list** — `weather_hourly_data_pipeline` toggled On, green status.
   `![DAGs list](screenshots/01_dags_list.png)`
2. **Grid view** — extract_data → transform_data → load_data all green.
   `![Grid view](screenshots/02_grid_success.png)`
3. **Graph view** — the task dependency chain.
   `![Graph view](screenshots/03_graph.png)`
4. **load_data log** — line "Successfully loaded 1 weather metrics record(s) into
   WEATHER_DB.RAW.CITY_WEATHER_METRICS".
   `![Load log](screenshots/04_load_log.png)`
5. **Snowflake result** — output of the verification `SELECT` above.
   `![Snowflake data](screenshots/05_snowflake_data.png)`
