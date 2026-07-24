-- ============================================================================
-- Destination table for the weather_hourly_data_pipeline (Snowflake)
-- Run this once in Snowsight (or SnowSQL) before enabling the Airflow DAG.
--
-- The Airflow load task also runs CREATE TABLE IF NOT EXISTS as a fallback,
-- so this script is the authoritative, documented DDL for submission.
-- ============================================================================

-- 1. Database + schema that hold the raw ingested data
CREATE DATABASE IF NOT EXISTS WEATHER_DB;
CREATE SCHEMA   IF NOT EXISTS WEATHER_DB.RAW;

-- 2. Destination table
--    Location metadata + ALL Open-Meteo current-weather fields.
--    SOURCE_NAME and LOAD_TIMESTAMP are the metadata columns added in transform.
CREATE TABLE IF NOT EXISTS WEATHER_DB.RAW.CITY_WEATHER_METRICS (
    CITY_NAME              STRING        NOT NULL,   -- logical location id
    RECORD_TIME            TIMESTAMP_NTZ,            -- API observation time
    LATITUDE               FLOAT,
    LONGITUDE              FLOAT,
    ELEVATION              FLOAT,                     -- metres
    TEMPERATURE_2M         FLOAT,                     -- air temp at 2m (C)
    APPARENT_TEMPERATURE   FLOAT,                     -- "feels like" (C)
    RELATIVE_HUMIDITY_2M   FLOAT,                     -- %
    IS_DAY                 NUMBER,                    -- 1 = day, 0 = night
    PRECIPITATION          FLOAT,                     -- mm
    RAIN                   FLOAT,                     -- mm
    SHOWERS                FLOAT,                     -- mm
    SNOWFALL               FLOAT,                     -- cm
    WEATHER_CODE           NUMBER,                    -- WMO weather code
    CLOUD_COVER            FLOAT,                     -- %
    PRESSURE_MSL           FLOAT,                     -- hPa (mean sea level)
    SURFACE_PRESSURE       FLOAT,                     -- hPa (surface)
    WIND_SPEED_10M         FLOAT,                     -- km/h
    WIND_DIRECTION_10M     FLOAT,                     -- degrees
    WIND_GUSTS_10M         FLOAT,                     -- km/h
    -- ---- metadata columns added by the transform step ----
    SOURCE_NAME            STRING        NOT NULL,    -- logical source identifier
    LOAD_TIMESTAMP         TIMESTAMP_NTZ NOT NULL     -- UTC time the row was loaded
);

-- 3. Verification queries (run after a DAG execution)
--    SELECT COUNT(*) AS row_count, MAX(LOAD_TIMESTAMP) AS last_loaded
--    FROM WEATHER_DB.RAW.CITY_WEATHER_METRICS;
--
--    SELECT * FROM WEATHER_DB.RAW.CITY_WEATHER_METRICS
--    ORDER BY LOAD_TIMESTAMP DESC LIMIT 10;
