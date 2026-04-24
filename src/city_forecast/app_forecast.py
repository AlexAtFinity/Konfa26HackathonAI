from __future__ import annotations

import os
from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

# Streamlit runs this file as a script (not as a package), so relative imports break.
# Add the repo's `src/` directory to `sys.path` so we can import `city_forecast.*`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from city_forecast.dbsql import has_databricks_creds, query_to_pandas  # noqa: E402


CATALOG = "fp_hack"
SCHEMA = "alexander_groth_hackathon"

FORECAST_TABLE = f"{CATALOG}.{SCHEMA}.city_daily_sales_forecast"
WEATHER_TABLE = f"{CATALOG}.{SCHEMA}.city_daily_weather_forecast"


def load_from_databricks(warehouse_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    forecast = query_to_pandas(f"SELECT * FROM {FORECAST_TABLE}", warehouse_id=warehouse_id)
    weather = query_to_pandas(f"SELECT * FROM {WEATHER_TABLE}", warehouse_id=warehouse_id)
    return forecast, weather


st.set_page_config(page_title="City Sales Forecast", layout="wide")
st.title("City-level 10-day sales forecast")

warehouse_id = st.text_input(
    "Databricks SQL Warehouse ID",
    value=os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
    help="Example: cfe55031a9b649cb",
)
use_databricks = has_databricks_creds() and bool(warehouse_id)
if not use_databricks:
    st.error(
        "Missing Databricks credentials and/or warehouse id. "
        "Set `DATABRICKS_WAREHOUSE_ID` and ensure Databricks CLI auth is configured."
    )
    st.stop()

forecast_df, weather_df = load_from_databricks(warehouse_id)

if forecast_df.empty:
    st.error("Forecast table/CSV is empty. Run the ML step first.")
    st.stop()

forecast_df["date"] = pd.to_datetime(forecast_df["date"]).dt.date
weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.date

joined = forecast_df.merge(
    weather_df,
    on=["city", "state", "date"],
    how="left",
    suffixes=("", "_weather"),
)

city_options = (
    joined[["city", "state"]]
    .drop_duplicates()
    .sort_values(["state", "city"])
    .apply(lambda r: f"{r['city']}, {r['state']}", axis=1)
    .tolist()
)

selected = st.selectbox("City", options=city_options, index=0)
selected_city, selected_state = [p.strip() for p in selected.split(",")]

view = joined[(joined["city"] == selected_city) & (joined["state"] == selected_state)].sort_values("date")

left, right = st.columns([1, 1])
with left:
    st.subheader("10-day forecast")
    st.dataframe(
        view[
            [
                "date",
                "sales_amount_pred",
                "temp_avg",
                "precip_total",
                "cloud_cover",
                "wind_speed",
                "humidity",
                "uv",
            ]
        ],
        use_container_width=True,
    )

with right:
    st.subheader("Predicted sales")
    fig = px.line(view, x="date", y="sales_amount_pred")
    fig.update_traces(mode="lines+markers")
    fig.update_xaxes(dtick="D1", tickformat="%b. %-d %a", title_text="")
    fig.update_yaxes(title_text="")
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

st.caption(f"Reads `{FORECAST_TABLE}` and `{WEATHER_TABLE}` when in Databricks mode.")
