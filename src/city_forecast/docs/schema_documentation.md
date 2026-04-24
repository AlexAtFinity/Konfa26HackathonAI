# Schema documentation & data quality findings (Databricks UC)

Date of inspection: **2026-04-24**

Canonical grain target for this project (per spec): **city + state + date**.

---

## 1) Forecast weather source

**Table:** `samples.accuweather.forecast_daily_calendar_metric`  
**Grain (observed):** `city_name` + `date` (unique; `COUNT(*) = COUNT(DISTINCT city_name|date)`).

**Key columns (observed types):**
- Location: `city_name` (string), `country_code` (string; observed lowercase like `us`), `latitude` (double), `longitude` (double)
- Date: `date` (date)
- Temperature: `temperature_max/min/avg` (double)
- Cloud cover: `cloud_cover_perc_avg/max/min` (int; observed range 0–100)
- Humidity: `humidity_relative_avg/max/min` (double; observed range ~9–96)
- Precipitation (liquid water equivalent): `precipitation_lwe_total` (double), plus `rain_lwe_total` (double), `snow_lwe_total` (double)
- Snow depth/amount: `snow_total` (double)
- UV: `index_uv_avg/max/min` (double)
- Wind: `wind_speed_avg/max/min` (double), `wind_gust_avg/max/min` (double), `wind_direction_avg` (double)

**Nullability / DQ checks (observed):**
- No nulls observed for `city_name`, `date`, `temperature_avg`, `precipitation_lwe_total` in this table snapshot.
- **Coverage issue for target cities:** only `los angeles` is present; `chicago`, `columbus`, `jacksonville`, `portland` have **0 rows** in this source.
- **Freshness issue:** rows for `los angeles` observed only for dates **2024-07-12 → 2024-07-26** (not “next 10 days” relative to 2026-04-24).
- **Join-key issue:** no `state` and no `postal_code` in this source; joining must be on `city_name` (and/or lat/long), which is ambiguous across states for some city names.

**Project output table (implemented proxy):** `fp_hack.alexander_groth_hackathon.city_daily_weather_forecast`  
**Grain (output):** `city` + `state` + `date` (10 days from `current_date()`).

Reason: the `samples` forecast table does not contain 4/5 target cities and is stale relative to 2026-04-24, so we generate a deterministic proxy forecast from the historical postal-code daily table (see `src/city_forecast/docs/forecast_weather_source_notes.md`).

---

## 2) Historical weather source

**Table:** `hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.historical.us_postal_daily_metric`  
**Grain (observed):** `POSTAL_CODE` + `DATE_CALENDAR` (unique; `COUNT(*) = COUNT(DISTINCT POSTAL_CODE|DATE_CALENDAR)`).

**Key columns (observed types):**
- Location: `POSTAL_CODE` (string), `COUNTRY_CODE` (string), `latitude` (decimal(5,3)), `longitude` (decimal(6,3))
- Date: `DATE_CALENDAR` (date)
- Temperature: `TEMPERATURE_MAX/MIN/AVG` (decimal(5,2))
- Cloud cover: `CLOUD_COVER_AVG/MAX/MIN` (decimal(3,2); observed range 0.00–1.00)
- Humidity: `HUMIDITY_RELATIVE_AVG/MAX/MIN` (decimal(5,2); observed range ~11.84–100.00)
- Precipitation (liquid water equivalent): `PRECIPITATION_LWE_TOTAL` (decimal(8,5)), `RAIN_LWE_TOTAL` (decimal(8,5)), `SNOW_LWE_TOTAL` (decimal(8,5))
- Snow: `SNOW_TOTAL` (decimal(8,5))
- UV: `INDEX_UV_AVG/MAX/MIN` (decimal(4,2))
- Wind: `WIND_SPEED_AVG/MAX/MIN` (decimal(5,2)), `WIND_GUST_AVG/MAX/MIN` (decimal(5,2)), `WIND_DIRECTION_AVG` (decimal(4,1))

**Nullability / DQ checks (observed):**
- No nulls observed for `POSTAL_CODE` and `DATE_CALENDAR`.
- **Join-key issue:** there is **no** `city` or `state` column; getting to city/state grain requires an external postal→city/state mapping (e.g., join via retail `customers.postcode` + `customers.city/state`, then aggregate postal-day → city-state-day).

**Project output table:** `fp_hack.alexander_groth_hackathon.city_daily_weather_history`  
**Grain (output):** `city` + `state` + `date`  
**Key columns (output):** `temp_max`, `temp_min`, `temp_avg`, `cloud_cover`, `precip_total`, `rain`, `snow`, `uv`, `humidity`, `wind_speed`, plus `postal_code_count`, `wind_gust_max_mph`, `unit_system`.

---

## 3) Forecast ↔ historical weather metric mapping

Intended mapping for a unified city/day feature set (names are as stored in the sources):

| Unified metric (suggested) | Forecast column (`samples...forecast_daily_calendar_metric`) | Historical column (`...us_postal_daily_metric`) | Notes / transforms |
|---|---|---|---|
| `temp_max` | `temperature_max` (double) | `TEMPERATURE_MAX` (decimal) | Units assumed metric; align types (cast to double/decimal). |
| `temp_min` | `temperature_min` | `TEMPERATURE_MIN` |  |
| `temp_avg` | `temperature_avg` | `TEMPERATURE_AVG` |  |
| `cloud_cover_avg` | `cloud_cover_perc_avg` (int, 0–100) | `CLOUD_COVER_AVG` (decimal, 0–1) | Convert one side: `forecast/100.0` **or** `historical*100.0`. |
| `precip_total_lwe` | `precipitation_lwe_total` | `PRECIPITATION_LWE_TOTAL` | Same “LWE” naming; verify downstream units before modeling. |
| `rain_total_lwe` | `rain_lwe_total` | `RAIN_LWE_TOTAL` |  |
| `snow_total` | `snow_total` | `SNOW_TOTAL` | `snow_lwe_total` also exists in both sources if preferred. |
| `uv_avg` | `index_uv_avg` | `INDEX_UV_AVG` |  |
| `humidity_avg` | `humidity_relative_avg` | `HUMIDITY_RELATIVE_AVG` | Both appear to be percent (0–100). |
| `wind_speed_avg` | `wind_speed_avg` | `WIND_SPEED_AVG` |  |

Gaps:
- Forecast includes probability fields (e.g., `rain_probability`), but historical daily table has no `*_PROBABILITY` columns.

---

## 4) Resolved customers table

**Project output table:** `fp_hack.alexander_groth_hackathon.target_city_customers_resolved`  
**Grain (output):** 1 row per `customer_id`.

**Source table:** `hackthon_group_a2_databricks_simulated_retail_customer_data.v01.customers`  
**Grain (source):** 1 row per `customer_id` (expected).

**Key columns (observed types):**
- `customer_id` (bigint), `customer_name` (string)
- Location: `state` (string), `city` (string), `postcode` (string), plus `lat/lon` (double)
- SCD-ish fields: `valid_from` (bigint), `valid_to` (string)

**Nullability / DQ checks (observed):**
- Total rows: **28,813**
- `city` null/blank: **4,869**
- `state` null/blank: **0**
- `postcode` null/blank: **33**
- `postcode` with decimal suffix (regex `\\.[0-9]+$`, e.g. `.0`): **13,153**

**Resolved table (expected grain):** 1 row per `customer_id` with non-null `city/state` for the 5 target cities; any postcode-based repair should normalize `postcode` first (strip trailing `.0`).

---

## 5) Daily sales table

**Project output table:** `fp_hack.alexander_groth_hackathon.city_daily_sales_history`  
**Grain (output):** `city` + `state` + `date` (January 2022 remap).

**Primary source table:** `hackthon_group_a2_databricks_simulated_retail_customer_data.v01.sales_orders`  
**Grain (source):** 1 row per `order_number` (expected).

**Key columns (observed types):**
- `order_number` (bigint), `customer_id` (bigint), `order_datetime` (bigint)
- `ordered_products` (string) — expected to be parsed to compute order value
- `number_of_line_items` (bigint)

**Nullability / DQ checks (observed):**
- Total rows: **4,074**
- `order_datetime` null: **45**
- `customer_id` null: **0**
- `ordered_products` null/blank: **0**

**Daily sales (expected grain):** `city + state + date` with a numeric daily sales amount (requires joining to resolved customers on `customer_id`, converting `order_datetime` to date, and aggregating; spec also remaps 2019 dates into **January 2022** while preserving day-of-month).

---

## 6) Final training table

**Project output table:** `fp_hack.alexander_groth_hackathon.city_daily_sales_weather_training`.

**Expected grain:** `city + state + date` (historical window).  
**Expected content:** daily sales target (e.g., `sales_amount`) + aligned historical weather features (temperature, cloud cover, precipitation, humidity, wind, UV, etc.).

**Key DQ risks to document/handle:**
- Postal→city mapping required for historical weather aggregation (historical weather table has no city/state).
- Cloud cover unit mismatch (forecast percent vs historical fraction).
- Missing `order_datetime` rows in `sales_orders` and potential JSON parsing failures in `ordered_products`.

---

## 7) Final forecast table

**Expected output table:** `fp_hack.alexander_groth_hackathon.city_daily_sales_forecast` *(not implemented yet in this repo)*.

**Expected grain:** `city + state + date` (forecast horizon).  
**Expected content:** forecast weather features + predicted sales (e.g., `sales_amount_pred`).

**Blocking DQ issues from current discovered forecast source:**
- `samples.accuweather.forecast_daily_calendar_metric` does not contain 4/5 required target cities, and its dates are in **July 2024** (not a rolling future horizon relative to 2026-04-24).
