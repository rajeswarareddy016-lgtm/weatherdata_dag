import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

DAG_ID = "weather_hourly_data_pipeline"

SOURCE_NAME = "Open_Meteo_Global_Weather_API"

# Open-Meteo "current weather" REST endpoint (returns a JSON "current" object).
API_URL = "https://api.open-meteo.com/v1/forecast"

# ALL available current-weather variables.
CURRENT_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "is_day",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "pressure_msl",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
]
API_PARAMS = {
    "latitude": 52.52,      # Berlin
    "longitude": 13.41,
    "current": ",".join(CURRENT_FIELDS),
}

# ---- Snowflake destination ----
# Airflow connection id (Admin -> Connections). Add one named 'snowflake_default'.
SNOWFLAKE_CONN_ID = "snowflake_default"
TARGET_TABLE = "WEATHER_DB.RAW.CITY_WEATHER_METRICS"


# ****dag arguments passing
default_args = {
    "owner": "rajesh",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def extract(**kwargs):
    logging.info("Starting Data Extraction Task: Requesting raw weather analytics payload.")
    try:
        response = requests.get(API_URL, params=API_PARAMS, timeout=12)
        response.raise_for_status()  # Instantly errors out if HTTP code is non-200

        raw_data = response.json()
        logging.info("Successfully extracted payload containing all current weather fields.")

        # Share raw nested data across the execution runtime
        kwargs['ti'].xcom_push(key='raw_extracted_weather', value=raw_data)

    except requests.exceptions.RequestException as e:
        logging.error(f"Extraction step completely failed due to network disruption: {str(e)}")
        raise


def _num(value):
    """Safe float conversion; returns None if the value is missing/null."""
    return None if value is None else float(value)


def transform(**kwargs):
    logging.info("Starting Data Transformation Task: Normalizing weather matrix records.")
    ti = kwargs['ti']

    raw_data = ti.xcom_pull(key='raw_extracted_weather', task_ids='extract_data')

    if not raw_data or 'current' not in raw_data:
        logging.error("Transformation failed: JSON structural keys missing or empty.")
        raise ValueError("Transformation failed: Malformed input array metadata.")

    current = raw_data['current']

    # Validation step: Confirm that core temperature variables exist
    if 'temperature_2m' not in current:
        logging.error("Validation failed: Target critical attribute 'temperature_2m' missing.")
        raise KeyError("Missing essential metric column variables.")

    transformed_records = []
    # Metadata column: ingestion audit timeline (UTC)
    load_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    # API observation time comes as ISO "2026-07-24T10:15"; make it Snowflake-friendly.
    record_time = str(current.get('time', '')).replace('T', ' ')

    # Full row: location metadata + ALL weather fields + ingestion metadata.
    # Column order MUST match the INSERT statement in load().
    transformed_row = (
        "Berlin_Global_Center",                        # CITY_NAME
        record_time,                                   # RECORD_TIME (observation)
        _num(raw_data.get('latitude')),                # LATITUDE
        _num(raw_data.get('longitude')),               # LONGITUDE
        _num(raw_data.get('elevation')),               # ELEVATION
        _num(current.get('temperature_2m')),           # TEMPERATURE_2M (C)
        _num(current.get('apparent_temperature')),     # APPARENT_TEMPERATURE (C)
        _num(current.get('relative_humidity_2m')),     # RELATIVE_HUMIDITY_2M (%)
        int(current.get('is_day', 0)),                 # IS_DAY (1/0)
        _num(current.get('precipitation')),            # PRECIPITATION (mm)
        _num(current.get('rain')),                     # RAIN (mm)
        _num(current.get('showers')),                  # SHOWERS (mm)
        _num(current.get('snowfall')),                 # SNOWFALL (cm)
        int(current.get('weather_code', 0)),           # WEATHER_CODE (WMO)
        _num(current.get('cloud_cover')),              # CLOUD_COVER (%)
        _num(current.get('pressure_msl')),             # PRESSURE_MSL (hPa)
        _num(current.get('surface_pressure')),         # SURFACE_PRESSURE (hPa)
        _num(current.get('wind_speed_10m')),           # WIND_SPEED_10M (km/h)
        _num(current.get('wind_direction_10m')),       # WIND_DIRECTION_10M (deg)
        _num(current.get('wind_gusts_10m')),           # WIND_GUSTS_10M (km/h)
        str(SOURCE_NAME),                              # SOURCE_NAME (metadata)
        str(load_time),                                # LOAD_TIMESTAMP (metadata)
    )

    transformed_records.append(transformed_row)
    logging.info(f"Successfully transformed and structured {len(transformed_records)} dataset rows.")

    ti.xcom_push(key='transformed_clean_weather', value=transformed_records)


def load(**kwargs):
    """
    LOAD STEP:
    - Fetches the clean tuple list from the transformation step.
    - Opens a Snowflake connection via the Airflow SnowflakeHook.
    - Creates the target table if needed and bulk-inserts the records.
    """
    logging.info("Starting Data Loading Task: Initializing Snowflake connection runtime.")
    ti = kwargs['ti']

    records_to_load = ti.xcom_pull(key='transformed_clean_weather', task_ids='transform_data')

    if not records_to_load:
        logging.warning("Zero clean data records found. Terminating load transaction.")
        return

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()

        # Inline fallback statement creates the table if initialization was skipped
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                CITY_NAME              STRING        NOT NULL,
                RECORD_TIME            TIMESTAMP_NTZ,
                LATITUDE               FLOAT,
                LONGITUDE              FLOAT,
                ELEVATION              FLOAT,
                TEMPERATURE_2M         FLOAT,
                APPARENT_TEMPERATURE   FLOAT,
                RELATIVE_HUMIDITY_2M   FLOAT,
                IS_DAY                 NUMBER,
                PRECIPITATION          FLOAT,
                RAIN                   FLOAT,
                SHOWERS                FLOAT,
                SNOWFALL               FLOAT,
                WEATHER_CODE           NUMBER,
                CLOUD_COVER            FLOAT,
                PRESSURE_MSL           FLOAT,
                SURFACE_PRESSURE       FLOAT,
                WIND_SPEED_10M         FLOAT,
                WIND_DIRECTION_10M     FLOAT,
                WIND_GUSTS_10M         FLOAT,
                SOURCE_NAME            STRING        NOT NULL,
                LOAD_TIMESTAMP         TIMESTAMP_NTZ NOT NULL
            );
        """)

        # 22 %s bind markers (Snowflake connector paramstyle), matching the tuple order
        insert_query = f"""
            INSERT INTO {TARGET_TABLE} VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """
        cursor.executemany(insert_query, records_to_load)
        conn.commit()  # Save records to Snowflake

        logging.info(
            f"Pipeline complete! Successfully loaded {len(records_to_load)} "
            f"weather metrics record(s) into: {TARGET_TABLE}."
        )

    except Exception as e:
        logging.error(f"Snowflake ingestion crashed during runtime load phase: {str(e)}")
        conn.rollback()  # Rollback to maintain clean state
        raise
    finally:
        conn.close()  # Close connection safely under all conditions


# ==========================================
# 4. AIRFLOW DAG PIPELINE ORCHESTRATION CONTEXT
# ==========================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Automated hourly data pipeline tracking global city weather changes.",
    schedule="@hourly",  # Executes automatically at the top of every hour
    catchup=False,
    max_active_runs=1,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load,
    )

    # Task dependency chain: extract -> transform -> load
    extract_task >> transform_task >> load_task
