{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_dm_deal_consumption', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DM_DEAL_CONSUMPTION',
        'target_table': 'ST_INT_DM_DEAL_CONSUMPTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.975432+00:00'
    }
) }}

WITH 

source_ff_int_dm_deal_consump AS (
    SELECT
        sales_path,
        deal_booking_net_usd,
        last_update_date,
        deal_id
    FROM {{ source('raw', 'ff_int_dm_deal_consump') }}
),

final AS (
    SELECT
        sales_path,
        deal_booking_net_usd,
        last_update_date,
        deal_id
    FROM source_ff_int_dm_deal_consump
)

SELECT * FROM final