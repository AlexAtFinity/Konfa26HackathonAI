# AccuWeather Historical Weather (Postal Code Sample)

## What this is
A static sample of AccuWeather historical weather data for **U.S. postal codes**, including:
- **Daily historical weather**
- **Hourly historical weather**
- **Normalized weather parameters**

This sample contains **~1 month** of daily and hourly records for U.S. postal codes (the upstream/product dataset is described as having daily/hourly history back to **2000**, but this share is a sample).

## What’s included (logical datasets / tables)
The share provides three datasets (names may vary in Databricks; discover actual table names via `SHOW TABLES`):
- Daily historical weather
- Hourly historical weather
- Normalized weather parameters

## Common fields / concepts
Expected fields and concepts across the datasets include:
- **Date / timestamp** (daily date for daily dataset; hourly timestamp for hourly dataset)
- **Postal code metadata**, including **latitude/longitude**
- **Daily aggregates** (e.g., temperature, “RealFeel”, wind speed/direction)
- **Observed/point-in-time parameters** (e.g., temperature, relative humidity, cloud cover %)

## Geographic coverage
Stated coverage: **United States and Canada** (this share is described as a U.S. postal code sample; validate coverage by inspecting distinct country/region fields if present).

## Typical use cases (examples)
- Join weather to business metrics (e.g., sales) by location + time to quantify weather impact.
- Demand/energy modeling using historical hourly and daily conditions.
- Retail planning/marketing analysis using historical conditions and seasonality.

## Notes for an AI agent (how to use this description)
- Use the **hourly** dataset for intra-day effects; use the **daily** dataset for longer horizon analyses and simpler joins.
- Align time carefully when joining to other data: decide on **local time vs UTC** and aggregate hourly → daily when needed.
- Do **not** assume exact column names/types from this document; inspect schemas with `DESCRIBE TABLE` and sample rows.
- Databricks access (Unity Catalog):
  - Catalog: `hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample`
  - Discover assets:
    - `SHOW SCHEMAS IN hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample;`
    - `SHOW TABLES IN hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.<schema>;`
    - `DESCRIBE TABLE hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.<schema>.<table>;`
  - Query example (replace placeholders after discovery):
    - `SELECT * FROM hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample.<schema>.<table> LIMIT 10;`
