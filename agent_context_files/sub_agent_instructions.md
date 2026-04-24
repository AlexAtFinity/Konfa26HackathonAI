# Agent Instructions for "Väder möter försäljning"

This file turns the tasks in [main_task.md](/Users/alexandergroth/git_repos/Konfa26HackathonAI/agent_context_files/main_task.md) into a multi-agent execution plan.

The goal is to produce a city-level 10-day sales forecast for five U.S. cities by combining forecast weather, historical weather, and historical retail sales, then expose the results in a simple browser app.

## Fixed scope

Use exactly these cities:

- Los Angeles, CA
- Chicago, IL
- Columbus, OH
- Jacksonville, FL
- Portland, OR

## Shared rules for every agent

- Use Databricks SQL and fully qualified Unity Catalog names.
- Do not assume schema or column names without verifying them first.
- Verify the actual forecast weather source up front. The provided weather description covers historical data; forecast data may live in a different table or source.
- Treat city plus state plus date as the canonical grain unless a task says otherwise.
- Preserve reproducibility: every important transformation should be expressible as SQL or code committed to the repo.
- When city values are missing in retail customer data, attempt postcode-based repair before excluding the row.
- Normalize dates carefully when historical sales are remapped from 2019 dates into January 2022.
- Document assumptions, filters, dropped rows, and unresolved data quality issues.
- Every output table should have a clear primary grain and a short schema description.

## Recommended agent split

There are seven sub-agents plus one coordinating lead. The lead can be a human or a top-level orchestration agent.

## Lead Agent

Mission:
Own sequencing, naming conventions, handoffs, and acceptance criteria.

Responsibilities:

- Confirm the actual Databricks table names and schemas before downstream work starts.
- Confirm where the forecast weather data actually comes from before assigning detailed downstream weather work.
- Choose canonical table names for intermediate and final outputs.
- Ensure all agents use the same city list, date grain, and field naming.
- Resolve conflicts in join keys, repaired city values, and weather metric definitions.
- Gate the handoff between data preparation, modeling, and app work.

Inputs:

- [main_task.md](/Users/alexandergroth/git_repos/Konfa26HackathonAI/agent_context_files/main_task.md)
- [Retail_data_description_from_databricks.md](/Users/alexandergroth/git_repos/Konfa26HackathonAI/agent_context_files/Retail_data_description_from_databricks.md)
- [Weather_data_descroiption_from_databricks.md](/Users/alexandergroth/git_repos/Konfa26HackathonAI/agent_context_files/Weather_data_descroiption_from_databricks.md)

Deliverables:

- Final execution order
- Canonical naming spec
- Acceptance checklist for all downstream tables and app outputs

## Agent 1: Forecast Weather Agent

Mission:
Build a clean 10-day forecast weather table for the five target cities.

Responsibilities:

- Discover the forecast weather source and verify available fields.
- Filter to the five target cities.
- Aggregate postcode-level forecast rows into one row per city, state, and date.
- Keep the requested weather fields where available:
  - postal code or zip code coverage reference
  - city
  - state
  - date
  - temperature max, min, avg
  - cloud cover
  - total precipitation
  - rain
  - snow
  - UV
  - humidity
  - wind
  - any additional useful fields if present
- Document the forecast schema and data types.

Output table:

- `city_daily_weather_forecast`

Expected grain:

- one row per city, state, date

Handoffs:

- Provide the cleaned forecast table to Agent 6 and Agent 7.

Key risks:

- Multiple postcodes per city causing duplicated city-day rows
- Inconsistent city spellings or state abbreviations
- Missing weather metrics for some forecast dates
- Forecast data source not being the same source family as historical weather

## Agent 2: Historical Weather Agent

Mission:
Build a historical daily weather table for the five target cities that is directly comparable to forecast weather.

Responsibilities:

- Inspect the catalog `hackthon_group_a2_accuweather_historical_weather_data_u_s_postal_codes_sample`.
- Discover which table best supports daily city-level aggregation.
- Match the historical weather indicators to the forecast weather indicators as closely as possible.
- Filter to the five target cities.
- Aggregate postcode-level historical weather into one row per city, state, and date.
- Produce a field mapping between historical and forecast weather columns.

Output table:

- `city_daily_weather_history`

Expected grain:

- one row per city, state, date

Handoffs:

- Provide the cleaned historical weather table and field mapping to Agent 6 and Agent 7.

Key risks:

- Daily versus hourly source mismatch
- Postal-code coverage differences across cities
- Indicator names or units differing between forecast and history

## Agent 3: Retail Customer Resolution Agent

Mission:
Prepare a city-resolved customer dimension for the target cities.

Responsibilities:

- Inspect `hackthon_group_a2_databricks_simulated_retail_customer_data.v01.customers`.
- Filter customer records to the five target cities.
- If city is null, attempt lookup or repair from postcode.
- If postcode values end with `.0`, strip the suffix before repair logic.
- Standardize city and state values for downstream joins.
- Produce a clear audit of:
  - total rows scanned
  - rows repaired by postcode
  - rows still missing usable location
  - rows retained for target cities

Output table:

- `target_city_customers_resolved`

Expected grain:

- one row per customer

Handoffs:

- Provide the resolved customer table to Agent 4.

Key risks:

- Dirty or mixed postcode formats
- Null city values with no recoverable postcode
- City names not matching weather-side conventions

## Agent 4: Sales Aggregation Agent

Mission:
Create the historical daily sales table for the five target cities.

Responsibilities:

- Join resolved customers from Agent 3 to `v01.sales_orders` on `customer_id`.
- Extract total sales amount from price information in ordered products.
- Convert `order_datetime` from integer to date.
- Use one full historical month ending on `2019-10-31`.
- Remap order dates from their original month and year into January 2022 while preserving day-of-month.
  - Example: `2019-10-14` becomes `2022-01-14`
- Aggregate to one row per city, state, and date.

Output table:

- `city_daily_sales_history`

Expected grain:

- one row per city, state, date

Handoffs:

- Provide the daily sales table to Agent 6 and Agent 7.

Key risks:

- Nested or semi-structured ordered product pricing
- Ambiguity between order-level and line-level totals
- Date remapping creating invalid or duplicate dates if not handled carefully

## Agent 5: Data Quality and Schema Agent

Mission:
Document schemas, types, and data quality findings across the weather and sales tables.

Responsibilities:

- Produce schema documentation for:
  - forecast weather source
  - historical weather source
  - resolved customer table
  - daily sales table
  - final joined modeling table
- Capture field meanings, data types, nullability observations, and grain.
- Record data issues and mitigation decisions so the modeling agent can trust the inputs.

Deliverables:

- `schema_documentation.md` or equivalent project artifact
- field mapping between forecast and historical weather metrics
- data quality report

Handoffs:

- Provide documentation to the Lead Agent, Agent 6, and Agent 7.

Key risks:

- Field drift between discovered source schema and downstream assumptions
- Silent type coercions

## Agent 6: Feature Join Agent

Mission:
Join historical weather and historical sales into the modeling dataset.

Responsibilities:

- Join Agent 2 output with Agent 4 output on city, state, and date.
- Resolve naming and type mismatches using Agent 5 documentation.
- Create derived features if needed:
  - rain indicator
  - temperature range
  - rolling sales lag
  - rolling weather summaries
  - weekend flag if useful
- Ensure the final modeling table contains one row per city, state, date with a target column for sales amount.

Output table:

- `city_daily_sales_weather_training`

Expected grain:

- one row per city, state, date

Handoffs:

- Provide the training dataset to Agent 7.

Key risks:

- Sparse overlap between weather and sales days
- Leakage from feature creation
- Broken joins from inconsistent city labels

## Agent 7: ML Forecasting Agent

Mission:
Train the best-suited sales prediction model and generate 10-day forecasts.

Responsibilities:

- Evaluate a small set of sensible baseline and stronger models.
  - Suggested order: linear regression baseline, random forest or gradient boosting, and optionally XGBoost if allowed
- Use the historical joined data from Agent 6.
- Select the best model based on validation metrics appropriate for daily sales prediction.
- Apply the winning model to the forecast weather table from Agent 1.
- Save forecasted sales amounts per day per city.

Outputs:

- trained model artifact
- validation metrics report
- `city_daily_sales_forecast`

Expected grain for forecast table:

- one row per city, state, forecast date

Handoffs:

- Provide the forecast table and evaluation summary to Agent 8.

Key risks:

- Very limited training window producing unstable estimates
- Weather-only features underfitting sales behavior
- Overfitting caused by too few city-day observations

## Agent 8: Browser App Agent

Mission:
Build a simple browser-accessible app for exploring forecasted sales and weather by city.

Responsibilities:

- Let the user choose one of the five cities.
- Show the next 10 days of:
  - forecast date
  - forecast weather summary
  - predicted sales amount
- Keep the app simple, reliable, and easy to demo.
- Prefer a minimal framework already compatible with the repo setup, such as Streamlit.
- Read only the finalized forecast output table or API produced downstream, not raw weather or retail source tables.

Inputs:

- `city_daily_sales_forecast`
- `city_daily_weather_forecast`
- validation summary from Agent 7

Deliverables:

- runnable app
- run instructions
- clear explanation of where the app reads its data from

Key risks:

- App depending on intermediate tables that change names
- Missing city-level forecast rows
- Poor demo clarity if weather and sales are shown separately rather than together
- Business logic leaking into the app because it reads raw or semi-processed source tables

## Dependency order

Run in this order unless discovery forces a change:

1. Lead Agent confirms source table names and naming conventions.
2. Agent 1 builds forecast weather table.
3. Agent 2 builds historical weather table.
4. Agent 3 resolves customer geography.
5. Agent 4 builds historical sales table.
6. Agent 5 documents schemas and data quality alongside the above.
7. Agent 6 builds the joined training dataset.
8. Agent 7 trains the model and generates forecasts.
9. Agent 8 builds the browser app.

## Suggested acceptance criteria

- Forecast weather table exists for all five cities and the coming 10 days.
- Historical weather table and historical sales table are both city-day grain.
- Sales history uses repaired customer geography where possible and documents losses.
- Joined training table is consistent, typed, and usable for ML.
- Model training includes at least one baseline and one non-linear model.
- Forecast output contains one row per city per forecast date.
- App shows all 10 forecast days for the selected city with both weather and predicted sales.

## Suggested file and artifact outputs

- SQL notebooks or scripts for each data agent
- one schema documentation artifact
- one modeling script or notebook
- one final app entrypoint
- one final README section describing:
  - tables created
  - model selected
  - how to run the app
  - known limitations

## Notes for the lead orchestrator

- Keep each sub-agent’s write scope narrow to avoid conflicts.
- Force every data-producing agent to state output grain explicitly.
- Require table samples and row counts at every handoff.
- Do not let the ML agent guess field meanings; require the mapping from Agent 5 first.
- If forecast and historical weather fields do not align perfectly, document the exact proxy features used.
- Keep the Browser App Agent downstream of the finalized forecast output so UI work does not reimplement analytics logic.
