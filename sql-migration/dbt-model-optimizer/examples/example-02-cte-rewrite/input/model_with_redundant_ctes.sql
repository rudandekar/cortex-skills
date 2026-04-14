{{ config(
    materialized='table',
    schema='mart'
) }}

WITH source_orders AS (

    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        total_amount
    FROM {{ ref('stg_orders') }}

),

unused_lookup AS (

    -- This CTE is defined but never referenced downstream
    SELECT
        lookup_key,
        lookup_value
    FROM {{ ref('ref_lookup_codes') }}
    WHERE category = 'ORDER_STATUS'

),

ranked AS (

    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) AS rn
    FROM source_orders

),

filtered AS (

    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        total_amount
    FROM ranked
    WHERE rn = 1

),

final AS (

    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        CURRENT_TIMESTAMP(0) AS EDWSF_CREATE_DTM,
        CURRENT_TIMESTAMP(0) AS EDWSF_UPDATE_DTM,
        CAST(CURRENT_USER() AS VARCHAR(30)) AS EDWSF_CREATE_USER,
        CAST(CURRENT_USER() AS VARCHAR(30)) AS EDWSF_UPDATE_USER
    FROM filtered

)

SELECT * FROM final
