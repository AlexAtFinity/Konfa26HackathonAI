# City Forecast Pipeline

This folder contains a Databricks-first pipeline (SQL + Python) to build a 10-day sales forecast for five U.S. cities by combining:

- AccuWeather forecast weather (Databricks `samples.accuweather.*`)
- AccuWeather historical weather (Databricks share)
- Simulated retail customer + orders data (Databricks share)

The canonical Unity Catalog schema used by the SQL scripts is:

- `fp_hack.alexander_groth_hackathon`

## Expected flow

1. Run SQL scripts in `src/city_forecast/sql/` in numeric order to create intermediate tables.
2. Run the Python ML step to train + generate `city_daily_sales_forecast`.
3. Run the Streamlit app to explore the forecasts.

## Run ML + write forecast table

First make sure your Databricks CLI/SDK auth is configured (see `instruction_files/fpaihack/databricks-access-instructions.md`).

```bash
UV_CACHE_DIR=/tmp/uv-cache uv sync

# Train + write forecast table back to UC
DATABRICKS_WAREHOUSE_ID=cfe55031a9b649cb UV_CACHE_DIR=/tmp/uv-cache uv run python -m src.city_forecast.ml_forecast --write-databricks
```

## Run app

```bash
DATABRICKS_WAREHOUSE_ID=cfe55031a9b649cb UV_CACHE_DIR=/tmp/uv-cache uv run streamlit run src/city_forecast/app_forecast.py
```
