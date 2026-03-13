-- models/marts/customers/mart_customer_segments.sql
-- ============================================================
-- NovaBrew Analytics — Mart: Customer Segments (RFM)
-- Post 2: Customer Segmentation
-- ============================================================
-- Builds RFM (Recency, Frequency, Monetary) scores for every
-- customer and assigns a segment label.
--
-- RFM scoring methodology:
--   R (Recency)   — Days since last order. Lower = better.
--   F (Frequency) — Total number of orders. Higher = better.
--   M (Monetary)  — Total revenue. Higher = better.
--
-- Each dimension is scored 1–4 using NTILE quartiles.
-- Combined RFM score drives the segment label.
--
-- Segments:
--   Champions      — R4, high F+M. Best customers.
--   Loyal          — High F, decent R. Regular buyers.
--   Promising      — Recent first purchase, low F.
--   Needs Attention— Mid R+F, declining engagement.
--   At Risk        — Used to be good, hasn't bought recently.
--   Lost           — Low R, low F. Churned.
--   Non-Buyer      — Never purchased.
-- ============================================================

WITH orders AS (

    SELECT * FROM {{ ref('stg_orders') }}
    WHERE is_revenue_order = TRUE

),

customers AS (

    SELECT * FROM {{ ref('stg_customers') }}

),

-- ── Step 1: Aggregate order stats per customer ─────────────

customer_orders AS (

    SELECT
        customer_id,
        COUNT(order_id)                              AS order_count,
        SUM(order_total)                             AS total_revenue,
        AVG(order_total)                             AS avg_order_value,
        MIN(order_date)                              AS first_order_date,
        MAX(order_date)                              AS last_order_date,
        DATE_DIFF(
            DATE '2024-06-30',
            MAX(order_date),
            DAY
        )                                            AS days_since_last_order

    FROM orders
    GROUP BY customer_id

),

-- ── Step 2: Join to customers (include non-purchasers) ─────

customer_base AS (

    SELECT
        c.customer_id,
        c.acquisition_channel,
        c.channel_type,
        c.state,
        c.age_band,
        c.is_subscriber,
        c.signup_date,

        COALESCE(o.order_count, 0)                   AS order_count,
        COALESCE(o.total_revenue, 0.0)               AS total_revenue,
        COALESCE(o.avg_order_value, 0.0)             AS avg_order_value,
        o.first_order_date,
        o.last_order_date,
        COALESCE(o.days_since_last_order, 999)       AS days_since_last_order,

        (o.customer_id IS NOT NULL)                  AS has_purchased

    FROM customers c
    LEFT JOIN customer_orders o USING (customer_id)

),

-- ── Step 3: Separate purchasers from non-purchasers ────────

purchasers AS (

    SELECT * FROM customer_base WHERE has_purchased = TRUE

),

-- ── Step 4: NTILE quartile scoring (1=worst, 4=best) ───────

rfm_scores AS (

    SELECT
        *,
        NTILE(4) OVER (ORDER BY days_since_last_order DESC)  AS r_score,
        NTILE(4) OVER (ORDER BY order_count ASC)             AS f_score,
        NTILE(4) OVER (ORDER BY total_revenue ASC)           AS m_score

    FROM purchasers

),

-- ── Step 5: Derive segment from RFM scores ─────────────────

segmented AS (

    SELECT
        *,
        CONCAT(
            CAST(r_score AS STRING),
            CAST(f_score AS STRING),
            CAST(m_score AS STRING)
        )                                            AS rfm_string,

        r_score + f_score + m_score                  AS rfm_total,

        CASE
            WHEN r_score = 4 AND f_score >= 3 AND m_score >= 3
                THEN 'Champion'
            WHEN f_score >= 3 AND r_score >= 2
                THEN 'Loyal'
            WHEN r_score >= 3 AND f_score <= 2 AND order_count <= 2
                THEN 'Promising'
            WHEN r_score = 2 AND f_score >= 2 AND m_score >= 2
                THEN 'Needs Attention'
            WHEN r_score <= 2 AND f_score >= 3
                THEN 'At Risk'
            WHEN r_score = 1 AND f_score <= 2
                THEN 'Lost'
            ELSE 'Occasional'
        END                                          AS segment

    FROM rfm_scores

),

-- ── Step 6: Non-purchasers with explicit NULL types ─────────
-- IMPORTANT: All NULL columns must be explicitly typed to match
-- the types in the segmented CTE for UNION ALL to work.

non_purchasers AS (

    SELECT
        customer_id,
        acquisition_channel,
        channel_type,
        state,
        age_band,
        is_subscriber,
        signup_date,
        order_count,
        total_revenue,
        avg_order_value,
        first_order_date,
        last_order_date,
        days_since_last_order,
        has_purchased,
        CAST(NULL AS INT64)                          AS r_score,
        CAST(NULL AS INT64)                          AS f_score,
        CAST(NULL AS INT64)                          AS m_score,
        CAST(NULL AS STRING)                         AS rfm_string,
        CAST(NULL AS INT64)                          AS rfm_total,
        'Non-Buyer'                                  AS segment

    FROM customer_base
    WHERE has_purchased = FALSE

),

-- ── Step 7: Combine and finalise ───────────────────────────

final AS (

    SELECT
        customer_id,
        acquisition_channel,
        channel_type,
        state,
        age_band,
        is_subscriber,
        signup_date,
        order_count,
        ROUND(total_revenue, 2)                      AS total_revenue,
        ROUND(avg_order_value, 2)                    AS avg_order_value,
        first_order_date,
        last_order_date,
        days_since_last_order,
        has_purchased,
        r_score,
        f_score,
        m_score,
        rfm_string,
        rfm_total,
        segment,
        CURRENT_TIMESTAMP()                          AS _updated_at

    FROM segmented

    UNION ALL

    SELECT
        customer_id,
        acquisition_channel,
        channel_type,
        state,
        age_band,
        is_subscriber,
        signup_date,
        order_count,
        ROUND(total_revenue, 2),
        ROUND(avg_order_value, 2),
        first_order_date,
        last_order_date,
        days_since_last_order,
        has_purchased,
        r_score,
        f_score,
        m_score,
        rfm_string,
        rfm_total,
        segment,
        CURRENT_TIMESTAMP()

    FROM non_purchasers

)

SELECT * FROM final
