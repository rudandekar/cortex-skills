{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_ff_int_dm_deal_consumption', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_FF_INT_DM_DEAL_CONSUMPTION',
        'target_table': 'FF_INT_DM_DEAL_CONSUMP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.870527+00:00'
    }
) }}

WITH 

source_int_dm_deal_consumption AS (
    SELECT
        sales_path,
        deal_booking_net_usd,
        last_update_date,
        deal_id
    FROM {{ source('raw', 'int_dm_deal_consumption') }}
),

final AS (
    SELECT
        sales_path,
        deal_booking_net_usd,
        last_update_date,
        deal_id
    FROM source_int_dm_deal_consumption
)

SELECT * FROM final