-- Four focused dashboard tiles for the dev schema. For test/prod, replace
-- workspace.weather_api_dev with the target data schema.

-- Tile 1: daily Gold weather trends by city.
SELECT forecast_date, location_name, avg_temperature_c, min_temperature_c,
       max_temperature_c, total_precipitation_mm, avg_wind_speed_kmh
FROM workspace.weather_api_dev.weather_daily_aggregation
ORDER BY forecast_date, location_name;

-- Tile 2: total precipitation by city across the dates currently in Gold.
SELECT location_name, SUM(total_precipitation_mm) AS total_precipitation_mm
FROM workspace.weather_api_dev.weather_daily_aggregation
GROUP BY location_name
ORDER BY total_precipitation_mm DESC;

-- Tile 3: recent pipeline executions with input, output, and rejected counts.
SELECT run_id, layer_type, status, start_time, end_time,
       input_row_count, output_row_count, rejected_row_count, reason
FROM workspace.audit.weather_job_audit
ORDER BY start_time DESC
LIMIT 50;

-- Tile 4: loaded-record freshness and quarantined row counts by city.
WITH locations AS (
  SELECT location_id, location_name
  FROM workspace.weather_api_dev.weather_hourly_transformed
  GROUP BY location_id, location_name
  UNION
  SELECT location_id, location_name
  FROM workspace.audit.weather_rejected_hourly_records
  GROUP BY location_id, location_name
), silver_health AS (
  SELECT location_id, COUNT(*) AS loaded_hourly_rows,
         MAX(forecast_time) AS latest_forecast_time
  FROM workspace.weather_api_dev.weather_hourly_transformed
  GROUP BY location_id
), rejected_health AS (
  SELECT location_id, COUNT(*) AS rejected_hourly_rows
  FROM workspace.audit.weather_rejected_hourly_records
  GROUP BY location_id
)
SELECT locations.location_name,
       COALESCE(silver_health.loaded_hourly_rows, 0) AS loaded_hourly_rows,
       silver_health.latest_forecast_time,
       COALESCE(rejected_health.rejected_hourly_rows, 0) AS rejected_hourly_rows
FROM locations
LEFT JOIN silver_health USING (location_id)
LEFT JOIN rejected_health USING (location_id)
ORDER BY locations.location_name;

-- Optional drill-down: rejected records and their quarantine reasons.
SELECT run_id, location_name, forecast_time, quarantine_reason
FROM workspace.audit.weather_rejected_hourly_records
ORDER BY forecast_time DESC
LIMIT 100;
