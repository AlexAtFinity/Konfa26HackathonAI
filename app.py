from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.weather_sales_pipeline import (
    DATASET_PATH,
    METRICS_PATH,
    load_dataset,
    load_model,
    train_model,
)


st.set_page_config(
    page_title="Väder möter försäljning",
    page_icon=":partly_sunny:",
    layout="wide",
)


@st.cache_data
def get_data() -> pd.DataFrame:
    if not Path(DATASET_PATH).exists():
        train_model()
    return load_dataset()


@st.cache_resource
def get_model():
    if not Path(METRICS_PATH).exists():
        train_model()
    return load_model()


df = get_data()
model = get_model()

st.title("Väder möter försäljning")
st.write(
    "En hackathon-MVP som simulerar hur väder, säsong och kampanjer påverkar "
    "försäljning. Byt gärna ut demo-datan mot AccuWeather + retail-data från Databricks Marketplace."
)

left, right = st.columns([1, 1.4], gap="large")

with left:
    st.subheader("Prediktera dagens försäljning")
    season = st.selectbox("Säsong", ["winter", "spring", "summer", "autumn"], index=2)
    temperature_c = st.slider("Temperatur (C)", min_value=-10, max_value=35, value=21)
    precipitation_mm = st.slider("Nederbörd (mm)", min_value=0.0, max_value=30.0, value=2.0, step=0.5)
    humidity_pct = st.slider("Luftfuktighet (%)", min_value=20, max_value=100, value=58)
    wind_speed_kmh = st.slider("Vindhastighet (km/h)", min_value=0, max_value=45, value=12)
    discount_pct = st.select_slider("Rabatt (%)", options=[0, 5, 10, 15, 20, 25], value=10)
    is_weekend = st.toggle("Helg", value=True)
    is_holiday = st.toggle("Helgdag", value=False)

    prediction_frame = pd.DataFrame(
        [
            {
                "season": season,
                "temperature_c": float(temperature_c),
                "precipitation_mm": float(precipitation_mm),
                "wind_speed_kmh": float(wind_speed_kmh),
                "humidity_pct": float(humidity_pct),
                "discount_pct": float(discount_pct),
                "is_weekend": int(is_weekend),
                "is_holiday": int(is_holiday),
                "is_rainy": int(precipitation_mm > 1.0),
            }
        ]
    )

    predicted_sales = model.predict(prediction_frame)[0]
    baseline_sales = df["sales_amount"].mean()
    delta = predicted_sales - baseline_sales

    st.metric("Predicerad försäljning", f"{predicted_sales:,.0f} kr", delta=f"{delta:+.0f} kr mot snitt")

    if Path(METRICS_PATH).exists():
        st.caption(Path(METRICS_PATH).read_text(encoding="utf-8"))

with right:
    st.subheader("Historisk demo-data")
    scatter = px.scatter(
        df,
        x="temperature_c",
        y="sales_amount",
        color="season",
        size="customer_count",
        hover_data=["precipitation_mm", "discount_pct", "is_weekend"],
        title="Temperatur vs. försäljning",
    )
    st.plotly_chart(scatter, use_container_width=True)

    trend = px.line(
        df.sort_values("date"),
        x="date",
        y="sales_amount",
        color="season",
        title="Försäljning över tid",
    )
    st.plotly_chart(trend, use_container_width=True)

st.subheader("Så använder du riktig data")
st.markdown(
    """
1. Exportera eller fråga ut AccuWeather-data och Simulated Retail Customer Data från Databricks.
2. Bygg en CSV med kolumnerna `season`, `temperature_c`, `precipitation_mm`, `wind_speed_kmh`,
   `humidity_pct`, `discount_pct`, `is_weekend`, `is_holiday`, `is_rainy`, `sales_amount`.
3. Kör `uv run python src/weather_sales_pipeline.py` för att träna om modellen.
4. Starta appen med `uv run streamlit run app.py`.
"""
)
