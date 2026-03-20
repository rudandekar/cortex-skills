{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fulflmnt_sol', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_ST_FULFLMNT_SOL',
        'target_table': 'ST_FULFLMNT_SOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.901745+00:00'
    }
) }}

WITH 

source_st_fulflmnt_sol AS (
    SELECT
        sales_order_line_key,
        concatenatd_sol_sku_lnkage_txt,
        bk_line_ref_num,
        previous_line_ref_num,
        bk_subscription_id,
        edw_update_datetime
    FROM {{ source('raw', 'st_fulflmnt_sol') }}
),

final AS (
    SELECT
        sales_order_line_key,
        concatenatd_sol_sku_lnkage_txt,
        bk_line_ref_num,
        previous_line_ref_num,
        bk_subscription_id,
        edw_update_datetime
    FROM source_st_fulflmnt_sol
)

SELECT * FROM final