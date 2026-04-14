{{ config(
    materialized='table',
    schema='mart',
    query_tag='model_with_redundant_ctes'
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
    FROM source_orders
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_date DESC
    ) = 1

)

SELECT * FROM final
