-- City-level 10-day weather forecast (deterministic proxy from historical sample)
-- Target: fp_hack.alexander_groth_hackathon.city_daily_weather_forecast
--
-- Source discovery outcome (as of script authoring):
-- - A forecast table exists in the Databricks `samples` catalog:
--     samples.accuweather.forecast_daily_calendar_metric
--   However, it does NOT contain the 5 required target cities and observed dates were in 2024,
--   so it cannot be used for "next 10 days" as of 2026-04-24.
-- - Best available *postal-code* weather source for all 5 cities is the historical daily sample:
--   hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.historical.us_postal_daily_imperial
--
-- Fallback/proxy method:
-- - Use representative postal codes for each requested city.
-- - Compute per-city averages by day-of-week over the available historical window.
-- - For each of the next 10 days (current_date() .. current_date()+9), apply the matching day-of-week average.
-- - If a city is missing any day-of-week in history, fall back to the city's overall historical average.
--
-- Notes on units (from *_imperial source):
-- - Temperature: Fahrenheit
-- - Wind speed/gust: mph
-- - Precipitation/rain/snow LWE totals: inches water equivalent
-- - Cloud cover: fraction 0..1 (converted to percent in output)
-- - Humidity: percent 0..100

CREATE SCHEMA IF NOT EXISTS fp_hack.alexander_groth_hackathon;

CREATE OR REPLACE TABLE fp_hack.alexander_groth_hackathon.city_daily_weather_forecast AS
WITH city_zip AS (
  SELECT * FROM VALUES
    ('Los Angeles', 'CA', '90012'),
    ('Chicago',     'IL', '60601'),
    ('Columbus',    'OH', '43215'),
    ('Jacksonville','FL', '32202'),
    ('Portland',    'OR', '97205')
  AS t(city, state, postal_code)
),
src AS (
  SELECT
    cz.city,
    cz.state,
    cz.postal_code,
    w.latitude,
    w.longitude,
    w.DATE_CALENDAR,
    dayofweek(w.DATE_CALENDAR) AS dow,

    CAST(w.TEMPERATURE_MAX AS DOUBLE) AS temp_max_f,
    CAST(w.TEMPERATURE_MIN AS DOUBLE) AS temp_min_f,
    CAST(w.TEMPERATURE_AVG AS DOUBLE) AS temp_avg_f,

    CAST(w.CLOUD_COVER_AVG AS DOUBLE) AS cloud_cover_avg_frac,

    CAST(w.PRECIPITATION_LWE_TOTAL AS DOUBLE) AS precip_total_in_lwe,
    CAST(w.RAIN_LWE_TOTAL AS DOUBLE)          AS rain_in_lwe,
    CAST(w.SNOW_LWE_TOTAL AS DOUBLE)          AS snow_in_lwe,

    CAST(w.INDEX_UV_MAX AS DOUBLE)            AS uv_index_max,
    CAST(w.HUMIDITY_RELATIVE_AVG AS DOUBLE)   AS humidity_avg_pct,

    CAST(w.WIND_SPEED_AVG AS DOUBLE)          AS wind_speed_avg_mph,
    CAST(w.WIND_SPEED_MAX AS DOUBLE)          AS wind_speed_max_mph,
    CAST(w.WIND_GUST_MAX AS DOUBLE)           AS wind_gust_max_mph,
    CAST(w.WIND_DIRECTION_PREDOMINANT AS DOUBLE) AS wind_direction_predominant_deg,

    CAST(w.MINUTES_OF_SUN_TOTAL AS DOUBLE)    AS minutes_of_sun_total
  FROM city_zip cz
  INNER JOIN hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.historical.us_postal_daily_imperial w
    ON w.COUNTRY_CODE = 'US'
   AND w.POSTAL_CODE = cz.postal_code
),
history_bounds AS (
  SELECT
    city,
    state,
    postal_code,
    MIN(DATE_CALENDAR) AS source_data_min_date,
    MAX(DATE_CALENDAR) AS source_data_max_date,
    COUNT(*) AS source_rows
  FROM src
  GROUP BY 1,2,3
),
hist_all AS (
  SELECT
    city,
    state,
    postal_code,
    MAX(latitude)  AS latitude,
    MAX(longitude) AS longitude,

    AVG(temp_max_f) AS temp_max_f,
    AVG(temp_min_f) AS temp_min_f,
    AVG(temp_avg_f) AS temp_avg_f,

    AVG(cloud_cover_avg_frac) AS cloud_cover_avg_frac,

    AVG(precip_total_in_lwe) AS precip_total_in_lwe,
    AVG(rain_in_lwe)         AS rain_in_lwe,
    AVG(snow_in_lwe)         AS snow_in_lwe,

    AVG(uv_index_max)               AS uv_index_max,
    AVG(humidity_avg_pct)           AS humidity_avg_pct,

    AVG(wind_speed_avg_mph)         AS wind_speed_avg_mph,
    AVG(wind_speed_max_mph)         AS wind_speed_max_mph,
    AVG(wind_gust_max_mph)          AS wind_gust_max_mph,
    AVG(wind_direction_predominant_deg) AS wind_direction_predominant_deg,

    AVG(minutes_of_sun_total)       AS minutes_of_sun_total
  FROM src
  GROUP BY 1,2,3
),
hist_dow AS (
  SELECT
    city,
    state,
    postal_code,
    dow,
    COUNT(*) AS hist_rows_for_dow,

    AVG(temp_max_f) AS temp_max_f,
    AVG(temp_min_f) AS temp_min_f,
    AVG(temp_avg_f) AS temp_avg_f,

    AVG(cloud_cover_avg_frac) AS cloud_cover_avg_frac,

    AVG(precip_total_in_lwe) AS precip_total_in_lwe,
    AVG(rain_in_lwe)         AS rain_in_lwe,
    AVG(snow_in_lwe)         AS snow_in_lwe,

    AVG(uv_index_max)               AS uv_index_max,
    AVG(humidity_avg_pct)           AS humidity_avg_pct,

    AVG(wind_speed_avg_mph)         AS wind_speed_avg_mph,
    AVG(wind_speed_max_mph)         AS wind_speed_max_mph,
    AVG(wind_gust_max_mph)          AS wind_gust_max_mph,
    AVG(wind_direction_predominant_deg) AS wind_direction_predominant_deg,

    AVG(minutes_of_sun_total)       AS minutes_of_sun_total
  FROM src
  GROUP BY 1,2,3,4
),
forecast_dates AS (
  SELECT
    d AS forecast_date,
    dayofweek(d) AS dow
  FROM (
    SELECT explode(sequence(current_date(), date_add(current_date(), 9), interval 1 day)) AS d
  )
)
SELECT
  cz.city,
  cz.state,
  fd.forecast_date AS date,

  cz.postal_code,
  ha.latitude,
  ha.longitude,

  -- unified feature names for downstream joins (imperial units; see unit_system column)
  COALESCE(hd.temp_max_f, ha.temp_max_f) AS temp_max,
  COALESCE(hd.temp_min_f, ha.temp_min_f) AS temp_min,
  COALESCE(hd.temp_avg_f, ha.temp_avg_f) AS temp_avg,

  (COALESCE(hd.cloud_cover_avg_frac, ha.cloud_cover_avg_frac) * 100.0) AS cloud_cover,

  COALESCE(hd.precip_total_in_lwe, ha.precip_total_in_lwe) AS precip_total,
  COALESCE(hd.rain_in_lwe,         ha.rain_in_lwe)         AS rain,
  COALESCE(hd.snow_in_lwe,         ha.snow_in_lwe)         AS snow,

  COALESCE(hd.uv_index_max, ha.uv_index_max) AS uv,
  COALESCE(hd.humidity_avg_pct, ha.humidity_avg_pct) AS humidity,

  COALESCE(hd.wind_speed_avg_mph, ha.wind_speed_avg_mph) AS wind_speed,
  COALESCE(hd.wind_speed_max_mph, ha.wind_speed_max_mph) AS wind_speed_max_mph,
  COALESCE(hd.wind_gust_max_mph,  ha.wind_gust_max_mph)  AS wind_gust_max_mph,
  COALESCE(hd.wind_direction_predominant_deg, ha.wind_direction_predominant_deg) AS wind_direction_predominant_deg,

  COALESCE(hd.minutes_of_sun_total, ha.minutes_of_sun_total) AS minutes_of_sun_total,

  COALESCE(hd.hist_rows_for_dow, 0) AS proxy_hist_rows_for_dow,
  hb.source_rows                  AS proxy_source_rows_total,
  hb.source_data_min_date,
  hb.source_data_max_date,

  'hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.historical.us_postal_daily_imperial' AS proxy_source_table,
  'avg_by_postal_code_and_dayofweek_over_available_history' AS proxy_method,
  'imperial' AS unit_system
FROM city_zip cz
CROSS JOIN forecast_dates fd
INNER JOIN hist_all ha
  ON ha.city = cz.city AND ha.state = cz.state AND ha.postal_code = cz.postal_code
INNER JOIN history_bounds hb
  ON hb.city = cz.city AND hb.state = cz.state AND hb.postal_code = cz.postal_code
LEFT JOIN hist_dow hd
  ON hd.city = cz.city AND hd.state = cz.state AND hd.postal_code = cz.postal_code AND hd.dow = fd.dow
;

-- Basic QA
SELECT city, state, COUNT(*) AS rows
FROM fp_hack.alexander_groth_hackathon.city_daily_weather_forecast
GROUP BY 1,2
ORDER BY 1,2;

SELECT COUNT(DISTINCT date) AS distinct_dates,
       MIN(date) AS min_date,
       MAX(date) AS max_date
FROM fp_hack.alexander_groth_hackathon.city_daily_weather_forecast;
