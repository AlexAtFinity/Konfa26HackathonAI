# Spår A — Avancerad Analys & Data Science

Typ av uppgifter: ML, prediktioner, avancerad analys

## 1. "Väder möter försäljning"

Kombinera AccuWeather-data med Simulated Retail Customer Data. Bygg en modell som predicerar hur väder påverkar köpbeteende — temperatur, regn, säsong.  
Marketplace: AccuWeather + Simulated Retail Customer Data

### Levererad MVP

Det finns nu en körbar MVP i repot för uppgiften:

- `src/weather_sales_pipeline.py` tränar en modell för att predicera försäljning utifrån väder, säsong och kampanjdata.
- `app.py` är en enkel Streamlit-app där man justerar väderparametrar och ser predicerad försäljning.
- Om riktig marketplace-data ännu inte är exporterad skapas en demo-dataset automatiskt som fallback.

### Kör projektet

```bash
uv sync
uv run python src/weather_sales_pipeline.py
uv run streamlit run app.py
```

### Förväntad indata för riktig träning

Byt ut demo-datan mot en CSV med följande kolumner:

`season`, `temperature_c`, `precipitation_mm`, `wind_speed_kmh`, `humidity_pct`, `discount_pct`, `is_weekend`, `is_holiday`, `is_rainy`, `sales_amount`

## 2. "Diabetes-prediktorn"

Bygg en ML-pipeline som identifierar riskfaktorer för diabetes. Feature engineering, modellträning, och en enkel webbapp där man matar in hälsovärden och får risknivå.  
Marketplace: Diabetes Health Indicators

## 3. "Vinkännaren"

Analysera Wine Quality Data med ML — vilka kemiska egenskaper gör ett bra vin? Bygg en interaktiv app där man justerar parametrar och ser predicerad kvalitet.  
Marketplace: Wine Quality Data

## 4. "Sjukdomskartan"

Visualisera Disease Prevalence Rates geografiskt. Korrelera med demografi från US Cities Demographics. Bygg en interaktiv karta med drill-down.  
Marketplace: Disease Prevalence Rates + US Cities Demographics

## 5. "Supply Chain-optimeraren"

Analysera Supply Chain Inventory and Transport-data. Hitta flaskhalsar, optimera lagernivåer, predicera leveransförseningar. Visualisera som en dashboard.  
Marketplace: Supply Chain Inventory and Transport

## 6. "FIFO-balansräknaren"

Testa en modell där balansen räknas med FIFO-metoden. Idag krävs 15 mått för att nå resultatet — row context och total context matchar inte på filter. Kan AI lösa det bättre och enklare?
