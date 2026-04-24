# Retail Synthetic Data (Databricks Share)

## What this is
This is a collection of **fully synthetic (fictional)** retail datasets intended for hands-on training with **Lakeflow Spark Declarative Pipelines (SDP)** and common ingestion patterns. Source files are provided as **CSV** and **JSON** to mimic real-world retail landing zones.

**Important:** No real customers, orders, or transactions are represented in this data.

## High-level structure
The share contains two schemas:
- `v01`
- `v02`

## Schema `v01`
Core “single company” retail records. These datasets accompany the *Get Started with Databricks for Data Analysis* course on Databricks Academy.

### Tables (curated)
- `customers`: Customers located in the US who purchase finished goods.
- `sales`: Individual item-level sales transactions.
- `sales_orders`: The originating purchase orders associated with each customer transaction.

### Volumes (raw / landing-zone style)
#### `source_files`
Raw CSV files used to build the three core tables:
- `customers.csv`
- `sales.csv`
- `sales_orders.csv`

#### `retail-pipeline`
Simulated streaming landing zone with JSON files in three subdirectories:
- `customers`: new / updated / deleted customers
- `orders`: order activity
- `status`: order status updates

## Schema `v02`
Seven days of daily “order drops” for **three fictional retail subsidiaries**, spanning **2025-11-01 through 2025-11-07**. Each subsidiary emits **one file per day**, and subsidiaries use different formats/structures.

Designed for workshops demonstrating:
- ingestion with **Auto Loader**
- **schema normalization** across heterogeneous sources
- other core SDP ingestion patterns from cloud storage

### Volumes (raw / landing-zone style)
#### `subsidiary_daily_orders`
Three directories, each simulating daily order drops:
- `bright_home_orders`: daily **CSV** files with home goods order activity
- `lumina_sports_orders`: daily **CSV** files for outdoor/athletic gear purchases
- `northstar_outfitters_orders`: daily **JSON** files for camping/travel product orders

#### `business_daily_events`
Unified cross-subsidiary business event stream; each day is a single mixed JSON file with three event groups:
- store operations
- marketing campaign activity
- logistics and fulfillment events

#### `customer_changes_daily`
Daily JSON customer change events (new signups, profile updates, deletions). Customers first appear on the day they are observed in subsidiary orders, providing a CDC-like feed aligned with downstream **SCD Type 2** processing and analytics.

## Notes for an AI agent (how to use this description)
- Treat `v01` as “curated tables + raw sources + streaming simulation”; treat `v02` as “multi-source daily drops + normalization + CDC/events”.
- Do **not** assume column names, keys, or types from this document alone; infer them by inspecting the file headers / JSON fields in the volumes (or table schemas if available in your environment).
- Databricks access (Unity Catalog):
  - Catalog: `hackthon_group_a2_databricks_simulated_retail_customer_data`
  - Discover assets:
    - `SHOW SCHEMAS IN hackthon_group_a2_databricks_simulated_retail_customer_data;`
    - `SHOW TABLES IN hackthon_group_a2_databricks_simulated_retail_customer_data.v01;`
    - `DESCRIBE TABLE hackthon_group_a2_databricks_simulated_retail_customer_data.v01.customers;`
  - Query example:
    - `SELECT * FROM hackthon_group_a2_databricks_simulated_retail_customer_data.v01.customers LIMIT 10;`
  - Volumes (if present) are typically accessible under:
    - `/Volumes/hackthon_group_a2_databricks_simulated_retail_customer_data/<schema>/<volume>/...`
