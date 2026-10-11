-- Dashboard query set for the dev schema. For test/prod, replace
-- main.weather_pipeline_dev with main.weather_pipeline_test/prod.

-- Tile 1: latest daily city comparison
SELECT forecast_date, location_name, avg_temperature_c, min_temperature_c,
       max_temperature_c, total_precipitation_mm, avg_wind_speed_kmh
FROM main.weather_pipeline_dev.weather_gold
ORDER BY forecast_date DESC, location_name;

-- Tile 2: precipitation by city and day
SELECT forecast_date, location_name, total_precipitation_mm
FROM main.weather_pipeline_dev.weather_gold
ORDER BY forecast_date DESC, location_name;

-- Tile 3: latest API outcome by city
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY location_id ORDER BY event_time DESC) AS rn
  FROM main.weather_pipeline_dev.weather_location_audit
)
SELECT location_name, status, event_time, error_message
FROM ranked WHERE rn = 1
ORDER BY status DESC, location_name;

-- Tile 4: ingested and rejected hourly records per run
SELECT b.run_id,
       count(DISTINCT concat_ws('|', b.location_id, b.fetched_at)) AS bronze_pulls,
       count(DISTINCT concat_ws('|', q.location_id, q.forecast_time)) AS rejected_hourly_rows,
       max(b.fetched_at) AS latest_bronze_fetch
FROM main.weather_pipeline_dev.weather_bronze b
LEFT JOIN main.weather_pipeline_dev.weather_quarantine q ON b.run_id = q.run_id
GROUP BY b.run_id
ORDER BY latest_bronze_fetch DESC;

-- Tile 5: hourly record freshness in Silver
SELECT location_name, max(forecast_time) AS latest_forecast_time,
       count(*) AS loaded_hourly_rows
FROM main.weather_pipeline_dev.weather_silver
GROUP BY location_name
ORDER BY location_name;

-- Tile 6: observed API schema changes
SELECT run_id, location_id, field_path, change_type,
       expected_type, observed_type, event_time
FROM main.weather_pipeline_dev.weather_schema_audit
ORDER BY event_time DESC;

-- Detail query: invalid records and quarantine reasons
SELECT run_id, location_name, forecast_time, quarantine_reason
FROM main.weather_pipeline_dev.weather_quarantine
ORDER BY forecast_time DESC;
