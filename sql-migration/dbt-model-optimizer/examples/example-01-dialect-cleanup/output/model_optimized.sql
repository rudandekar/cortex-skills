{{ config(
    materialized='table',
    schema='normalized',
    query_tag='n_order_header'
) }}

WITH source_data AS (

    SELECT
        order_id,
        customer_id,
        COALESCE(discount_amount, 0) AS discount_amount,
        COALESCE(tax_amount, 0) AS tax_amount,
        CASE
            WHEN status_code = 'A' THEN 'Active'
            WHEN status_code = 'I' THEN 'Inactive'
            WHEN status_code = 'P' THEN 'Pending'
            ELSE 'Unknown'
        END AS status_desc,
        SUBSTRING(region_code, 1, 2) AS region_short,
        CURRENT_DATE() AS load_date,
        order_date,
        total_amount
    FROM {{ source('raw_sales', 'N_ORDER_HEADER') }}

),

final AS (

    SELECT
        order_id,
        customer_id,
        discount_amount,
        tax_amount,
        status_desc,
        region_short,
        load_date,
        order_date,
        total_amount,
        total_amount - discount_amount + tax_amount AS net_amount,
        CURRENT_TIMESTAMP(0) AS EDWSF_CREATE_DTM,
        CURRENT_TIMESTAMP(0) AS EDWSF_UPDATE_DTM,
        CAST(CURRENT_USER() AS VARCHAR(30)) AS EDWSF_CREATE_USER,
        CAST(CURRENT_USER() AS VARCHAR(30)) AS EDWSF_UPDATE_USER,
        {{ var('batch_id', 0) }} AS EDWSF_BATCH_ID
    FROM source_data

)

SELECT * FROM final
