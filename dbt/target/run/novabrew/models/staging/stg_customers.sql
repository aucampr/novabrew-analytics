

  create or replace view `novabrew-analytics`.`novabrew_dev_novabrew_staging`.`stg_customers`
  OPTIONS()
  as -- models/staging/stg_customers.sql
-- ============================================================
-- NovaBrew Analytics — Staging: Customers
-- Post 2: Customer Segmentation
-- ============================================================
-- Cleans and types the raw customers table.
-- - Normalises channel names
-- - Derives age and age_band
-- - Masks PII fields (email hashed, names kept for dev only)
-- ============================================================

WITH source AS (

    SELECT * FROM `novabrew-analytics`.`novabrew_raw`.`customers`

),

cleaned AS (

    SELECT
        customer_id,

        -- PII — hash email in non-dev environments
        
            LOWER(TRIM(email))                       AS email_hash,
            INITCAP(TRIM(first_name))                AS first_name,
            INITCAP(TRIM(last_name))                 AS last_name,
        

        UPPER(TRIM(state))                           AS state,

        -- Normalise acquisition channel labels
        CASE LOWER(TRIM(acquisition_channel))
            WHEN 'paid_search'    THEN 'paid_search'
            WHEN 'paid_social'    THEN 'paid_social'
            WHEN 'organic_search' THEN 'organic_search'
            WHEN 'organic_social' THEN 'organic_social'
            WHEN 'email'          THEN 'email'
            WHEN 'direct'         THEN 'direct'
            WHEN 'referral'       THEN 'referral'
            ELSE 'unknown'
        END                                          AS acquisition_channel,

        CAST(signup_date AS DATE)                    AS signup_date,
        is_subscriber,
        email_opt_in,
        CAST(date_of_birth AS DATE)                  AS date_of_birth,

        -- Derived fields
        DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR)
                                                     AS age,

        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) < 25
                THEN '18-24'
            WHEN DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) < 35
                THEN '25-34'
            WHEN DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) < 45
                THEN '35-44'
            WHEN DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) < 55
                THEN '45-54'
            ELSE '55+'
        END                                          AS age_band,

        -- Channel grouping (top-level)
        CASE LOWER(TRIM(acquisition_channel))
            WHEN 'paid_search'    THEN 'paid'
            WHEN 'paid_social'    THEN 'paid'
            ELSE 'organic'
        END                                          AS channel_type

    FROM source
    WHERE customer_id IS NOT NULL

)

SELECT * FROM cleaned;

