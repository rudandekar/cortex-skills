{{ config(
    materialized='table',
    schema='normalized'
) }}

WITH source_data AS (

    SELECT
        order_id,
        customer_id,
        NVL(discount_amount, 0) AS discount_amount,
        NVL(tax_amount, 0) AS tax_amount,
        DECODE(status_code, 'A', 'Active', 'I', 'Inactive', 'P', 'Pending', 'Unknown') AS status_desc,
        SUBSTR(region_code, 1, 2) AS region_short,
        SYSDATE AS load_date,
        order_date,
        total_amount
    FROM $$STGDB.RAW_SALES.N_ORDER_HEADER

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
        EDW_CREATE_DTM,
        EDW_UPDATE_DTM,
        EDW_CREATE_USER,
        EDW_UPDATE_USER
    FROM source_data

)

SELECT * FROM final
