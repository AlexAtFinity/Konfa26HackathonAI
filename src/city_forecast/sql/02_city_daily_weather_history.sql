-- Creates/overwrites: fp_hack.alexander_groth_hackathon.city_daily_weather_history
-- Output grain: one row per city, state, date
--
-- Source (postal-code daily history, imperial units):
--   hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.historical.us_postal_daily_imperial
--
-- Fixed scope (exactly these cities):
--   - Los Angeles, CA
--   - Chicago, IL
--   - Columbus, OH
--   - Jacksonville, FL
--   - Portland, OR
--
-- Postal -> city mapping approach (required because the historical source does not include city/state columns):
--   - The historical daily table contains POSTAL_CODE + latitude/longitude but no city/state attributes.
--   - We map each POSTAL_CODE to the nearest of the five fixed city centers using Haversine distance.
--   - We only keep postal codes within 80km of a city center.
--
-- Assumptions / limitations:
--   - "Nearest city center within 80km" is an approximation of postal-code membership for this demo.
--   - This may include some suburbs/outskirts and exclude far-flung metro postal codes, especially for large metros.
--
-- Column mapping (Historical -> Forecast-aligned fields)
-- Notes:
--   - This script produces column names intended to match the forecast table's semantics.
--   - Cloud cover in the source appears to be a 0..1 fraction; we convert to 0..100 percent.
--   - Precipitation/rain in the imperial table are stored as LWE (liquid-water-equivalent) totals; we treat them as inches.
--
--   Forecast/Output column          Historical source / derivation
--   -----------------------------   -----------------------------------------------
--   city                            derived from postal->city mapping (city_centers.city)
--   state                           derived from postal->city mapping (city_centers.state)
--   date                            DATE_CALENDAR
--   postal_code_count               COUNT(DISTINCT POSTAL_CODE) within city/day
--   temp_max                        AVG(TEMPERATURE_MAX)
--   temp_min                        AVG(TEMPERATURE_MIN)
--   temp_avg                        AVG(TEMPERATURE_AVG)
--   cloud_cover                     AVG(CLOUD_COVER_AVG) * 100
--   precip_total                    AVG(PRECIPITATION_LWE_TOTAL)
--   rain                            AVG(RAIN_LWE_TOTAL)
--   snow                            AVG(SNOW_TOTAL)
--   uv                              AVG(INDEX_UV_MAX)
--   humidity                        AVG(HUMIDITY_RELATIVE_AVG)
--   wind_speed                      AVG(WIND_SPEED_AVG)
--   wind_gust_max_mph               AVG(WIND_GUST_MAX)
--

CREATE SCHEMA IF NOT EXISTS fp_hack.alexander_groth_hackathon;

CREATE OR REPLACE TABLE fp_hack.alexander_groth_hackathon.city_daily_weather_history AS
WITH city_centers AS (
  SELECT * FROM VALUES
    ('Los Angeles',    'CA', 34.0522, -118.2437),
    ('Chicago',        'IL', 41.8781,  -87.6298),
    ('Columbus',       'OH', 39.9612,  -82.9988),
    ('Jacksonville',   'FL', 30.3322,  -81.6557),
    ('Portland',       'OR', 45.5152, -122.6784)
  AS t(city, state, center_lat, center_lon)
),
postal_codes AS (
  SELECT DISTINCT
    POSTAL_CODE,
    CAST(latitude AS DOUBLE) AS lat,
    CAST(longitude AS DOUBLE) AS lon
  FROM hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.historical.us_postal_daily_imperial
  WHERE COUNTRY_CODE = 'US'
),
postal_city_ranked AS (
  SELECT
    p.POSTAL_CODE,
    c.city,
    c.state,
    2 * 6371 * asin(
      sqrt(
        pow(sin(radians(p.lat - c.center_lat) / 2), 2)
        + cos(radians(c.center_lat)) * cos(radians(p.lat))
          * pow(sin(radians(p.lon - c.center_lon) / 2), 2)
      )
    ) AS distance_km,
    row_number() OVER (
      PARTITION BY p.POSTAL_CODE
      ORDER BY 2 * 6371 * asin(
        sqrt(
          pow(sin(radians(p.lat - c.center_lat) / 2), 2)
          + cos(radians(c.center_lat)) * cos(radians(p.lat))
            * pow(sin(radians(p.lon - c.center_lon) / 2), 2)
        )
      )
    ) AS rn
  FROM postal_codes p
  CROSS JOIN city_centers c
),
postal_city_map AS (
  SELECT
    POSTAL_CODE,
    city,
    state
  FROM postal_city_ranked
  WHERE rn = 1
    AND distance_km <= 80
),
weather_postal AS (
  SELECT
    m.city,
    m.state,
    w.POSTAL_CODE,
    w.DATE_CALENDAR,
    w.TEMPERATURE_MAX,
    w.TEMPERATURE_MIN,
    w.TEMPERATURE_AVG,
    w.CLOUD_COVER_AVG,
    w.PRECIPITATION_LWE_TOTAL,
    w.RAIN_LWE_TOTAL,
    w.SNOW_TOTAL,
    w.INDEX_UV_MAX,
    w.HUMIDITY_RELATIVE_AVG,
    w.WIND_SPEED_AVG,
    w.WIND_GUST_MAX
  FROM hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.historical.us_postal_daily_imperial w
  INNER JOIN postal_city_map m
    ON w.POSTAL_CODE = m.POSTAL_CODE
  WHERE w.COUNTRY_CODE = 'US'
)
SELECT
  city,
  state,
  DATE_CALENDAR AS date,
  COUNT(DISTINCT POSTAL_CODE) AS postal_code_count,
  ROUND(AVG(CAST(TEMPERATURE_MAX AS DOUBLE)), 2) AS temp_max,
  ROUND(AVG(CAST(TEMPERATURE_MIN AS DOUBLE)), 2) AS temp_min,
  ROUND(AVG(CAST(TEMPERATURE_AVG AS DOUBLE)), 2) AS temp_avg,
  ROUND(AVG(CAST(CLOUD_COVER_AVG AS DOUBLE)) * 100.0, 1) AS cloud_cover,
  ROUND(AVG(CAST(PRECIPITATION_LWE_TOTAL AS DOUBLE)), 5) AS precip_total,
  ROUND(AVG(CAST(RAIN_LWE_TOTAL AS DOUBLE)), 5) AS rain,
  ROUND(AVG(CAST(SNOW_TOTAL AS DOUBLE)), 5) AS snow,
  ROUND(AVG(CAST(INDEX_UV_MAX AS DOUBLE)), 2) AS uv,
  ROUND(AVG(CAST(HUMIDITY_RELATIVE_AVG AS DOUBLE)), 2) AS humidity,
  ROUND(AVG(CAST(WIND_SPEED_AVG AS DOUBLE)), 2) AS wind_speed,
  ROUND(AVG(CAST(WIND_GUST_MAX AS DOUBLE)), 2) AS wind_gust_max_mph,
  'imperial' AS unit_system
FROM weather_postal
GROUP BY city, state, DATE_CALENDAR;

-- ---------------------------------------------------------------------------
-- QA queries
-- ---------------------------------------------------------------------------

-- Row count should be (#cities * #days) if every city has complete coverage.
SELECT
  city,
  state,
  COUNT(*) AS day_rows,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM fp_hack.alexander_groth_hackathon.city_daily_weather_history
GROUP BY city, state
ORDER BY city, state;

-- Duplicate key check (should return 0 rows).
SELECT
  city,
  state,
  date,
  COUNT(*) AS n
FROM fp_hack.alexander_groth_hackathon.city_daily_weather_history
GROUP BY city, state, date
HAVING COUNT(*) > 1;

-- Null check on core weather fields.
SELECT
  SUM(CASE WHEN temp_avg IS NULL THEN 1 ELSE 0 END) AS null_temp_avg,
  SUM(CASE WHEN precip_total IS NULL THEN 1 ELSE 0 END) AS null_precip_total,
  SUM(CASE WHEN wind_speed IS NULL THEN 1 ELSE 0 END) AS null_wind_speed,
  SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END) AS null_humidity
FROM fp_hack.alexander_groth_hackathon.city_daily_weather_history;
