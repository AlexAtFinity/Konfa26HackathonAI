# Forecast weather source notes (2026-04-24)

## What we found

- Databricks provides AccuWeather forecast tables in `samples.accuweather.*`, including `samples.accuweather.forecast_daily_calendar_metric`.
- For our fixed scope (Los Angeles, CA; Chicago, IL; Columbus, OH; Jacksonville, FL; Portland, OR), this source **only contained Los Angeles** and the available dates were in **July 2024**, so it cannot serve a rolling “next 10 days” forecast as of **2026-04-24**.
- The AccuWeather *historical postal code sample* catalog contains daily weather by `POSTAL_CODE` and `DATE_CALENDAR` and can cover all five cities after aggregation/mapping.

## Implemented fallback

To keep the pipeline runnable and deterministic, `fp_hack.alexander_groth_hackathon.city_daily_weather_forecast` is generated as a **proxy forecast**:

- Pick one representative ZIP per city.
- Compute historical averages by **day-of-week** for each ZIP over the available window.
- Apply those averages to `current_date()` .. `current_date()+9`.

This is a placeholder for a real forecast feed; it preserves the required output grain (`city,state,date`) and allows end-to-end joins/modeling.

