{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_summary_quote_sol_alloc', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_SUMMARY_QUOTE_SOL_ALLOC',
        'target_table': 'SM_SUMMARY_QUOTE_SOL_ALLOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.174621+00:00'
    }
) }}

WITH 

source_sm_summary_quote_sol_alloc AS (
    SELECT
        sk_sq_sol_alloc_key,
        sales_order_line_key,
        sales_motion_cd,
        dv_effective_dtm,
        sk_offer_attribution_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_summary_quote_sol_alloc') }}
),

final AS (
    SELECT
        sk_sq_sol_alloc_key,
        sales_order_line_key,
        sales_motion_cd,
        dv_effective_dtm,
        sk_offer_attribution_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_summary_quote_sol_alloc
)

SELECT * FROM final