{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ar_napld_rtr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_AR_NAPLD_RTR',
        'target_table': 'WI_AR_NAPLD_RTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.612233+00:00'
    }
) }}

WITH 

source_wi_ar_napld_rtr AS (
    SELECT
        ar_trx_key,
        deal_id,
        edw_create_dtm
    FROM {{ source('raw', 'wi_ar_napld_rtr') }}
),

final AS (
    SELECT
        ar_trx_key,
        deal_id,
        edw_create_dtm
    FROM source_wi_ar_napld_rtr
)

SELECT * FROM final