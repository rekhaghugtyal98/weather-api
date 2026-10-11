-- Run in the Databricks SQL editor or a notebook attached to the target workspace.
-- Change weather_api_dev to weather_api_test or weather_api_prod as needed.

CREATE SCHEMA IF NOT EXISTS workspace.weather_api_dev;
CREATE SCHEMA IF NOT EXISTS workspace.audit;

CREATE TABLE IF NOT EXISTS workspace.weather_api_dev.weather_api_raw (
  run_id STRING NOT NULL,
  location_id STRING NOT NULL,
  location_name STRING NOT NULL,
  latitude STRING,
  longitude STRING,
  fetched_at STRING NOT NULL,
  payload_json STRING NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.weather_api_dev.weather_hourly_transformed (
  run_id STRING,
  location_id STRING,
  location_name STRING,
  forecast_time STRING,
  temperature_c DOUBLE,
  humidity_pct INT,
  precipitation_mm DOUBLE,
  wind_speed_kmh DOUBLE,
  ingested_at STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.weather_api_dev.weather_daily_aggregation (
  location_id STRING,
  location_name STRING,
  forecast_date DATE,
  avg_temperature_c DOUBLE,
  min_temperature_c DOUBLE,
  max_temperature_c DOUBLE,
  avg_humidity_pct DOUBLE,
  total_precipitation_mm DOUBLE,
  avg_wind_speed_kmh DOUBLE,
  hourly_record_count BIGINT,
  updated_at STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.audit.weather_rejected_hourly_records (
  run_id STRING,
  location_id STRING,
  location_name STRING,
  forecast_time STRING,
  temperature_c DOUBLE,
  humidity_pct INT,
  precipitation_mm DOUBLE,
  wind_speed_kmh DOUBLE,
  ingested_at STRING,
  quarantine_reason STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.audit.weather_ingestion_audit (
  run_id STRING NOT NULL,
  location_id STRING NOT NULL,
  location_name STRING NOT NULL,
  status STRING NOT NULL,
  error_message STRING,
  event_time STRING NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.audit.weather_api_schema_changes (
  run_id STRING NOT NULL,
  location_id STRING NOT NULL,
  field_path STRING NOT NULL,
  change_type STRING NOT NULL,
  expected_type STRING,
  observed_type STRING,
  event_time STRING NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.audit.weather_job_audit (
  audit_id STRING NOT NULL,
  run_id STRING NOT NULL,
  layer_type STRING NOT NULL,
  status STRING NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  reason STRING,
  parameters_json STRING,
  input_row_count BIGINT,
  output_row_count BIGINT,
  rejected_row_count BIGINT
) USING DELTA;
