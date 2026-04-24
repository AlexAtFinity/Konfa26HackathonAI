-- Create daily city-level sales history for one full historical month ending 2019-10-31,
-- remapped into January 2022 (preserving day-of-month).
--
-- Dependencies:
-- - fp_hack.alexander_groth_hackathon.target_city_customers_resolved
--     Expected columns (per v01.customers): customer_id, city, state
-- - hackthon_group_a2_databricks_simulated_retail_customer_data.v01.sales_orders
--     ordered_products is a JSON array; price/qty are strings; promotion_info.promo_disc is a fractional discount (e.g., 0.03).

CREATE OR REPLACE TABLE fp_hack.alexander_groth_hackathon.city_daily_sales_history AS
WITH sales_orders_typed AS (
  SELECT
    so.order_number,
    so.customer_id,
    to_date(from_unixtime(so.order_datetime)) AS order_date,
    so.ordered_products
  FROM hackthon_group_a2_databricks_simulated_retail_customer_data.v01.sales_orders so
  WHERE so.order_datetime IS NOT NULL
    AND so.ordered_products IS NOT NULL
),
sales_orders_oct_2019 AS (
  SELECT *
  FROM sales_orders_typed
  WHERE order_date BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'
),
order_items AS (
  SELECT
    order_number,
    customer_id,
    order_date,
    explode(
      from_json(
        ordered_products,
        'array<struct<curr:string,id:string,name:string,price:string,promotion_info:struct<promo_disc:double,promo_id:string,promo_item:string,promo_qty:string>,qty:string,unit:string>>'
      )
    ) AS item
  FROM sales_orders_oct_2019
),
order_amounts AS (
  SELECT
    order_number,
    customer_id,
    order_date,
    SUM(
      CAST(
        CAST(item.price AS DECIMAL(18,2))
        * CAST(item.qty AS DECIMAL(18,2))
        * (CAST(1 AS DECIMAL(18,6)) - CAST(COALESCE(item.promotion_info.promo_disc, 0.0) AS DECIMAL(18,6)))
        AS DECIMAL(18,2)
      )
    ) AS order_sales_amount
  FROM order_items
  GROUP BY order_number, customer_id, order_date
),
orders_with_city AS (
  SELECT
    c.city,
    c.state,
    make_date(2022, 1, day(o.order_date)) AS date,
    o.order_sales_amount
  FROM order_amounts o
  INNER JOIN fp_hack.alexander_groth_hackathon.target_city_customers_resolved c
    ON o.customer_id = c.customer_id
  WHERE c.city IS NOT NULL AND c.state IS NOT NULL
)
SELECT
  city,
  state,
  date,
  SUM(order_sales_amount) AS sales_amount
FROM orders_with_city
GROUP BY city, state, date
;

-- ======================
-- QA queries (run after)
-- ======================

-- QA1: Ensure January 2022 mapping spans 31 days.
SELECT
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(DISTINCT date) AS distinct_days
FROM fp_hack.alexander_groth_hackathon.city_daily_sales_history
;

-- QA2: Ensure primary grain uniqueness (no duplicate city/state/date rows).
SELECT
  city,
  state,
  date,
  COUNT(*) AS row_count
FROM fp_hack.alexander_groth_hackathon.city_daily_sales_history
GROUP BY city, state, date
HAVING COUNT(*) > 1
;

-- QA3: Sanity-check that source dates were restricted to 2019-10-01..2019-10-31.
-- (This re-derives source order_date from sales_orders; should show 2019-10-01 and 2019-10-31.)
SELECT
  MIN(to_date(from_unixtime(order_datetime))) AS min_order_date,
  MAX(to_date(from_unixtime(order_datetime))) AS max_order_date,
  COUNT(*) AS rows_in_range
FROM hackthon_group_a2_databricks_simulated_retail_customer_data.v01.sales_orders
WHERE order_datetime IS NOT NULL
  AND ordered_products IS NOT NULL
  AND to_date(from_unixtime(order_datetime)) BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'
;

-- QA4: Check join coverage (orders in Oct 2019 that do NOT map to resolved target-city customers).
WITH order_amounts_oct_2019 AS (
  SELECT
    so.order_number,
    so.customer_id,
    to_date(from_unixtime(so.order_datetime)) AS order_date
  FROM hackthon_group_a2_databricks_simulated_retail_customer_data.v01.sales_orders so
  WHERE so.order_datetime IS NOT NULL
    AND so.ordered_products IS NOT NULL
    AND to_date(from_unixtime(so.order_datetime)) BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'
)
SELECT
  COUNT(*) AS orders_oct_2019,
  SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS orders_without_resolved_customer
FROM order_amounts_oct_2019 o
LEFT JOIN fp_hack.alexander_groth_hackathon.target_city_customers_resolved c
  ON o.customer_id = c.customer_id
;
