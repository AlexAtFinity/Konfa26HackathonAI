-- 03_target_city_customers_resolved.sql
--
-- Purpose
--   Build a resolved customer dimension for the five target cities:
--     - Los Angeles, CA
--     - Chicago, IL
--     - Columbus, OH
--     - Jacksonville, FL
--     - Portland, OR
--
-- Output
--   fp_hack.alexander_groth_hackathon.target_city_customers_resolved
--     Grain: one row per customer_id
--
-- City/state repair approach
--   - Clean postcode: trim; if it ends with '.0' strip that suffix; then extract 5-digit ZIP where possible.
--   - Build a postcode -> (city,state) reference from non-null rows in the same customers table
--     by picking the most frequent city/state for each ZIP (best-effort, since no external ZIP reference is available here).
--   - Only fill missing city/state; do not overwrite existing non-null values.
--   - Standardize output strings for join consistency: city = Initcap, state = upper.

CREATE SCHEMA IF NOT EXISTS fp_hack.alexander_groth_hackathon;

CREATE OR REPLACE TABLE fp_hack.alexander_groth_hackathon.target_city_customers_resolved AS
WITH target_city AS (
  SELECT * FROM VALUES
    ('los angeles', 'CA'),
    ('chicago',     'IL'),
    ('columbus',    'OH'),
    ('jacksonville','FL'),
    ('portland',    'OR')
  AS t(target_city_norm, target_state)
),
customers_ranked AS (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY customer_id
      ORDER BY (CASE WHEN valid_to IS NULL OR trim(valid_to) = '' THEN 1 ELSE 0 END) DESC,
               valid_from DESC
    ) AS rn
  FROM hackthon_group_a2_databricks_simulated_retail_customer_data.v01.customers c
),
customers_dedup AS (
  SELECT
    customer_id,
    tax_id,
    tax_code,
    customer_name,
    street,
    number,
    unit,
    region,
    district,
    lon,
    lat,
    ship_to_address,
    valid_from,
    valid_to,
    units_purchased,
    loyalty_segment,

    -- Clean postcode and extract 5-digit ZIP
    regexp_replace(trim(postcode), '\\.0$', '') AS postcode_clean,
    CASE
      WHEN regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1) <> ''
        THEN lpad(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1), 5, '0')
      ELSE nullif(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '(\\d{5})', 1), '')
    END AS postcode_5,

    -- Normalized city/state for matching + repair
    lower(nullif(trim(city), ''))  AS city_norm,
    upper(nullif(trim(state), '')) AS state_norm
  FROM customers_ranked
  WHERE rn = 1
),
postcode_city_state_counts AS (
  SELECT
    CASE
      WHEN regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1) <> ''
        THEN lpad(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1), 5, '0')
      ELSE nullif(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '(\\d{5})', 1), '')
    END AS postcode_5,
    lower(nullif(trim(city), ''))  AS ref_city_norm,
    upper(nullif(trim(state), '')) AS ref_state_norm,
    count(*) AS n
  FROM hackthon_group_a2_databricks_simulated_retail_customer_data.v01.customers
  WHERE (
      regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1) <> ''
      OR nullif(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '(\\d{5})', 1), '') IS NOT NULL
    )
    AND lower(nullif(trim(city), '')) IS NOT NULL
    AND upper(nullif(trim(state), '')) IS NOT NULL
  GROUP BY 1, 2, 3
),
postcode_city_state_ref AS (
  SELECT postcode_5, ref_city_norm, ref_state_norm
  FROM (
    SELECT
      postcode_5,
      ref_city_norm,
      ref_state_norm,
      n,
      row_number() OVER (
        PARTITION BY postcode_5
        ORDER BY n DESC, ref_city_norm, ref_state_norm
      ) AS rn
    FROM postcode_city_state_counts
  ) x
  WHERE rn = 1
),
customers_resolved AS (
  SELECT
    d.*,
    coalesce(d.city_norm,  r.ref_city_norm)  AS resolved_city_norm,
    coalesce(d.state_norm, r.ref_state_norm) AS resolved_state_norm,
    (d.city_norm  IS NULL AND r.ref_city_norm  IS NOT NULL) AS city_repaired_by_postcode,
    (d.state_norm IS NULL AND r.ref_state_norm IS NOT NULL) AS state_repaired_by_postcode
  FROM customers_dedup d
  LEFT JOIN postcode_city_state_ref r
    ON d.postcode_5 = r.postcode_5
),
customers_target_city AS (
  SELECT
    customer_id,
    customer_name,
    tax_id,
    tax_code,

    initcap(resolved_city_norm) AS city,
    resolved_state_norm         AS state,

    postcode_clean AS postcode,
    postcode_5,

    street,
    number,
    unit,
    region,
    district,
    lon,
    lat,
    ship_to_address,
    valid_from,
    valid_to,
    units_purchased,
    loyalty_segment,

    city_repaired_by_postcode,
    state_repaired_by_postcode
  FROM customers_resolved
  INNER JOIN target_city t
    ON customers_resolved.resolved_city_norm = t.target_city_norm
   AND customers_resolved.resolved_state_norm = t.target_state
)
SELECT *
FROM customers_target_city
;

-- --------------------
-- Audit / QA Queries
-- --------------------

-- A) Row grain check (should be 0)
SELECT
  COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_customer_id_rows
FROM fp_hack.alexander_groth_hackathon.target_city_customers_resolved;

-- B) Retained customer counts by target city/state
SELECT city, state, COUNT(*) AS customers
FROM fp_hack.alexander_groth_hackathon.target_city_customers_resolved
GROUP BY 1,2
ORDER BY 1,2;

-- C) Repair summary within retained target customers
SELECT
  COUNT(*) AS retained_customers,
  SUM(CASE WHEN city_repaired_by_postcode OR state_repaired_by_postcode THEN 1 ELSE 0 END) AS retained_customers_repaired,
  SUM(CASE WHEN city_repaired_by_postcode THEN 1 ELSE 0 END) AS retained_city_repaired,
  SUM(CASE WHEN state_repaired_by_postcode THEN 1 ELSE 0 END) AS retained_state_repaired
FROM fp_hack.alexander_groth_hackathon.target_city_customers_resolved;

-- D) End-to-end audit: scanned / missing / repaired / still missing / retained
WITH customers_ranked AS (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY customer_id
      ORDER BY (CASE WHEN valid_to IS NULL OR trim(valid_to) = '' THEN 1 ELSE 0 END) DESC,
               valid_from DESC
    ) AS rn
  FROM hackthon_group_a2_databricks_simulated_retail_customer_data.v01.customers c
),
customers_dedup AS (
  SELECT
    customer_id,
    regexp_replace(trim(postcode), '\\.0$', '') AS postcode_clean,
    CASE
      WHEN regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1) <> ''
        THEN lpad(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1), 5, '0')
      ELSE nullif(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '(\\d{5})', 1), '')
    END AS postcode_5,
    lower(nullif(trim(city), ''))  AS city_norm,
    upper(nullif(trim(state), '')) AS state_norm
  FROM customers_ranked
  WHERE rn = 1
),
postcode_city_state_ref AS (
  SELECT postcode_5, ref_city_norm, ref_state_norm
  FROM (
    SELECT
      postcode_5,
      ref_city_norm,
      ref_state_norm,
      n,
      row_number() OVER (
        PARTITION BY postcode_5
        ORDER BY n DESC, ref_city_norm, ref_state_norm
      ) AS rn
    FROM (
      SELECT
        CASE
          WHEN regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1) <> ''
            THEN lpad(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1), 5, '0')
          ELSE nullif(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '(\\d{5})', 1), '')
        END AS postcode_5,
        lower(nullif(trim(city), ''))  AS ref_city_norm,
        upper(nullif(trim(state), '')) AS ref_state_norm,
        count(*) AS n
      FROM hackthon_group_a2_databricks_simulated_retail_customer_data.v01.customers
      WHERE (
          regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '^(\\d{4})$', 1) <> ''
          OR nullif(regexp_extract(regexp_replace(trim(postcode), '\\.0$', ''), '(\\d{5})', 1), '') IS NOT NULL
        )
        AND lower(nullif(trim(city), '')) IS NOT NULL
        AND upper(nullif(trim(state), '')) IS NOT NULL
      GROUP BY 1, 2, 3
    ) postcode_city_state_counts
  ) x
  WHERE rn = 1
),
customers_resolved AS (
  SELECT
    d.*,
    coalesce(d.city_norm,  r.ref_city_norm)  AS resolved_city_norm,
    coalesce(d.state_norm, r.ref_state_norm) AS resolved_state_norm,
    (d.city_norm  IS NULL AND r.ref_city_norm  IS NOT NULL) AS city_repaired_by_postcode,
    (d.state_norm IS NULL AND r.ref_state_norm IS NOT NULL) AS state_repaired_by_postcode
  FROM customers_dedup d
  LEFT JOIN postcode_city_state_ref r
    ON d.postcode_5 = r.postcode_5
),
target_city AS (
  SELECT * FROM VALUES
    ('los angeles', 'CA'),
    ('chicago',     'IL'),
    ('columbus',    'OH'),
    ('jacksonville','FL'),
    ('portland',    'OR')
  AS t(target_city_norm, target_state)
),
customers_target_city AS (
  SELECT r.*
  FROM customers_resolved r
  INNER JOIN target_city t
    ON r.resolved_city_norm = t.target_city_norm
   AND r.resolved_state_norm = t.target_state
)
SELECT
  COUNT(*) AS customers_scanned,
  SUM(CASE WHEN city_norm IS NULL OR state_norm IS NULL THEN 1 ELSE 0 END) AS customers_missing_city_or_state,
  SUM(CASE WHEN (city_norm IS NULL OR state_norm IS NULL) AND postcode_5 IS NULL THEN 1 ELSE 0 END) AS missing_city_or_state_without_zip5,
  SUM(CASE WHEN city_repaired_by_postcode OR state_repaired_by_postcode THEN 1 ELSE 0 END) AS customers_repaired_by_postcode,
  SUM(CASE WHEN resolved_city_norm IS NULL OR resolved_state_norm IS NULL THEN 1 ELSE 0 END) AS customers_still_missing_after_repair,
  (SELECT COUNT(*) FROM customers_target_city) AS customers_retained_target_cities,
  (SELECT SUM(CASE WHEN city_repaired_by_postcode OR state_repaired_by_postcode THEN 1 ELSE 0 END) FROM customers_target_city) AS retained_customers_repaired
FROM customers_resolved
;
