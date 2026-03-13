-- models/staging/stg_orders.sql
-- ============================================================
-- NovaBrew Analytics — Staging: Orders
-- Post 2: Customer Segmentation
-- ============================================================
-- Cleans and types the raw orders table.
-- - Filters out cancelled/refunded orders from revenue calcs
--   (keeps them as records with is_revenue_order = false)
-- - Derives order_month, order_year, days_to_ship
-- - Validates that order_total > 0 for delivered orders
-- ============================================================

WITH source AS (

    SELECT * FROM {{ source('novabrew_raw', 'orders') }}

),

cleaned AS (

    SELECT
        order_id,
        customer_id,

        CAST(order_date AS DATE)                     AS order_date,
        CAST(shipped_date AS DATE)                   AS shipped_date,
        CAST(delivered_date AS DATE)                 AS delivered_date,

        -- Normalise channel
        CASE LOWER(TRIM(channel))
            WHEN 'paid_search'    THEN 'paid_search'
            WHEN 'paid_social'    THEN 'paid_social'
            WHEN 'organic_search' THEN 'organic_search'
            WHEN 'organic_social' THEN 'organic_social'
            WHEN 'email'          THEN 'email'
            WHEN 'direct'         THEN 'direct'
            WHEN 'referral'       THEN 'referral'
            ELSE 'unknown'
        END                                          AS channel,

        UPPER(TRIM(COALESCE(promo_code, '')))        AS promo_code,
        COALESCE(discount_pct, 0.0)                  AS discount_pct,
        COALESCE(subtotal, 0.0)                      AS subtotal,
        COALESCE(shipping_cost, 0.0)                 AS shipping_cost,
        COALESCE(tax, 0.0)                           AS tax,
        COALESCE(order_total, 0.0)                   AS order_total,

        LOWER(TRIM(status))                          AS status,
        COALESCE(is_first_order, FALSE)              AS is_first_order,

        -- Derived fields
        DATE_TRUNC(CAST(order_date AS DATE), MONTH)  AS order_month,

        EXTRACT(YEAR FROM CAST(order_date AS DATE))  AS order_year,

        EXTRACT(QUARTER FROM CAST(order_date AS DATE))
                                                     AS order_quarter,

        DATE_DIFF(
            CAST(shipped_date AS DATE),
            CAST(order_date AS DATE),
            DAY
        )                                            AS days_to_ship,

        -- Revenue flag — only count delivered orders in revenue metrics
        (LOWER(TRIM(status)) = 'delivered')          AS is_revenue_order,

        -- Promo flag
        (promo_code IS NOT NULL AND TRIM(promo_code) != '')
                                                     AS has_promo

    FROM source
    WHERE order_id IS NOT NULL

)

SELECT * FROM cleaned
