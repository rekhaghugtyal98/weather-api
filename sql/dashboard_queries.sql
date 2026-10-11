-- Dashboard query set for the dev schema. For test/prod, replace
-- workspace.weather_api_dev with workspace.weather_api_test/prod.

-- Tile 1: latest daily city comparison
SELECT forecast_date, location_name, avg_temperature_c, min_temperature_c,
       max_temperature_c, total_precipitation_mm, avg_wind_speed_kmh
FROM workspace.weather_api_dev.weather_daily_aggregation
ORDER BY forecast_date DESC, location_name;

-- Tile 2: precipitation by city and day
SELECT forecast_date, location_name, total_precipitation_mm
FROM workspace.weather_api_dev.weather_daily_aggregation
ORDER BY forecast_date DESC, location_name;

-- Tile 3: latest API outcome by city
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY location_id ORDER BY event_time DESC) AS rn
  FROM workspace.audit.weather_ingestion_audit
)
SELECT location_name, status, event_time, error_message
FROM ranked WHERE rn = 1
ORDER BY status DESC, location_name;

-- Tile 4: ingested and rejected hourly records per run
SELECT b.run_id,
       count(DISTINCT concat_ws('|', b.location_id, b.fetched_at)) AS bronze_pulls,
       count(DISTINCT concat_ws('|', q.location_id, q.forecast_time)) AS rejected_hourly_rows,
       max(b.fetched_at) AS latest_bronze_fetch
FROM workspace.weather_api_dev.weather_api_raw b
LEFT JOIN workspace.audit.weather_rejected_hourly_records q ON b.run_id = q.run_id
GROUP BY b.run_id
ORDER BY latest_bronze_fetch DESC;

-- Tile 5: hourly record freshness in Silver
SELECT location_name, max(forecast_time) AS latest_forecast_time,
       count(*) AS loaded_hourly_rows
FROM workspace.weather_api_dev.weather_hourly_transformed
GROUP BY location_name
ORDER BY location_name;

-- Tile 6: observed API schema changes
SELECT run_id, location_id, field_path, change_type,
       expected_type, observed_type, event_time
FROM workspace.audit.weather_api_schema_changes
ORDER BY event_time DESC;

-- Tile 7: one job status row per execution; reruns have separate audit IDs
SELECT run_id, layer_type, status, start_time, end_time, reason
FROM workspace.audit.weather_job_audit
ORDER BY start_time DESC;

-- Detail query: invalid records and quarantine reasons
SELECT run_id, location_name, forecast_time, quarantine_reason
FROM workspace.audit.weather_rejected_hourly_records
ORDER BY forecast_time DESC;
