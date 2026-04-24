-- 06_city_daily_sales_weather_training.sql
--
-- Purpose
--   Join daily historical sales to daily historical weather to create an ML training dataset.
--
-- Inputs (must exist before running this script)
--   - fp_hack.alexander_groth_hackathon.city_daily_sales_history
--       Expected grain: one row per city,state,date (or will be re-aggregated here).
--       Required columns: city, state, date, sales_amount
--   - fp_hack.alexander_groth_hackathon.city_daily_weather_history
--       Expected grain: one row per city,state,date (or will be re-aggregated here).
--       Required columns: city, state, date
--       Expected feature columns (adjust upstream tables to match these names):
--         temp_max, temp_min, temp_avg, cloud_cover, precip_total, rain, snow, uv, humidity, wind_speed
--
-- Output
--   - fp_hack.alexander_groth_hackathon.city_daily_sales_weather_training
--       Grain: one row per city,state,date

CREATE OR REPLACE TABLE fp_hack.alexander_groth_hackathon.city_daily_sales_weather_training AS
WITH sales_dedup AS (
  SELECT
    upper(trim(city)) AS city_key,
    upper(trim(state)) AS state_key,
    CAST(date AS DATE) AS date,
    SUM(CAST(sales_amount AS DOUBLE)) AS sales_amount
  FROM fp_hack.alexander_groth_hackathon.city_daily_sales_history
  GROUP BY 1, 2, 3
),
sales_features AS (
  SELECT
    city_key,
    state_key,
    date,
    sales_amount,
    LAG(sales_amount, 1) OVER (PARTITION BY city_key, state_key ORDER BY date) AS sales_amount_lag_1d,
    AVG(sales_amount) OVER (
      PARTITION BY city_key, state_key
      ORDER BY date
      ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS sales_amount_roll7_mean_prev
  FROM sales_dedup
),
weather_dedup AS (
  SELECT
    upper(trim(city)) AS city_key,
    upper(trim(state)) AS state_key,
    CAST(date AS DATE) AS date,
    AVG(CAST(temp_max AS DOUBLE)) AS temp_max,
    AVG(CAST(temp_min AS DOUBLE)) AS temp_min,
    AVG(CAST(temp_avg AS DOUBLE)) AS temp_avg,
    AVG(CAST(cloud_cover AS DOUBLE)) AS cloud_cover,
    AVG(CAST(precip_total AS DOUBLE)) AS precip_total,
    AVG(CAST(rain AS DOUBLE)) AS rain,
    AVG(CAST(snow AS DOUBLE)) AS snow,
    AVG(CAST(uv AS DOUBLE)) AS uv,
    AVG(CAST(humidity AS DOUBLE)) AS humidity,
    AVG(CAST(wind_speed AS DOUBLE)) AS wind_speed
  FROM fp_hack.alexander_groth_hackathon.city_daily_weather_history
  GROUP BY 1, 2, 3
)
SELECT
  initcap(lower(s.city_key)) AS city,
  s.state_key AS state,
  s.date,
  s.sales_amount,
  w.temp_max,
  w.temp_min,
  w.temp_avg,
  w.cloud_cover,
  w.precip_total,
  w.rain,
  w.snow,
  w.uv,
  w.humidity,
  w.wind_speed,
  (w.temp_max - w.temp_min) AS temp_range,
  CASE WHEN dayofweek(s.date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
  CASE WHEN COALESCE(w.rain, w.precip_total, 0.0) > 0.0 THEN 1 ELSE 0 END AS is_rainy,
  s.sales_amount_lag_1d,
  s.sales_amount_roll7_mean_prev
FROM sales_features s
INNER JOIN weather_dedup w
  ON s.city_key = w.city_key
 AND s.state_key = w.state_key
 AND s.date = w.date
;

-- -----------------------
-- QA / Validation Queries
-- -----------------------

-- 1) Row counts per input + output
SELECT COUNT(*) AS sales_rows
FROM fp_hack.alexander_groth_hackathon.city_daily_sales_history;

SELECT COUNT(*) AS weather_rows
FROM fp_hack.alexander_groth_hackathon.city_daily_weather_history;

SELECT COUNT(*) AS training_rows
FROM fp_hack.alexander_groth_hackathon.city_daily_sales_weather_training;

-- 2) Duplicate key checks (should return 0 rows)
SELECT city, state, CAST(date AS DATE) AS date, COUNT(*) AS cnt
FROM fp_hack.alexander_groth_hackathon.city_daily_sales_history
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;

SELECT city, state, CAST(date AS DATE) AS date, COUNT(*) AS cnt
FROM fp_hack.alexander_groth_hackathon.city_daily_weather_history
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;

SELECT city, state, date, COUNT(*) AS cnt
FROM fp_hack.alexander_groth_hackathon.city_daily_sales_weather_training
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;

-- 3) Join coverage (sales without weather / weather without sales)
WITH sales_dedup AS (
  SELECT
    upper(trim(city)) AS city_key,
    upper(trim(state)) AS state_key,
    CAST(date AS DATE) AS date
  FROM fp_hack.alexander_groth_hackathon.city_daily_sales_history
  GROUP BY 1, 2, 3
),
weather_dedup AS (
  SELECT
    upper(trim(city)) AS city_key,
    upper(trim(state)) AS state_key,
    CAST(date AS DATE) AS date
  FROM fp_hack.alexander_groth_hackathon.city_daily_weather_history
  GROUP BY 1, 2, 3
)
SELECT
  SUM(CASE WHEN w.date IS NULL THEN 1 ELSE 0 END) AS sales_keys_missing_weather,
  SUM(CASE WHEN s.date IS NULL THEN 1 ELSE 0 END) AS weather_keys_missing_sales,
  COUNT(*) AS total_distinct_keys_union
FROM sales_dedup s
FULL OUTER JOIN weather_dedup w
  ON s.city_key = w.city_key
 AND s.state_key = w.state_key
 AND s.date = w.date
;
