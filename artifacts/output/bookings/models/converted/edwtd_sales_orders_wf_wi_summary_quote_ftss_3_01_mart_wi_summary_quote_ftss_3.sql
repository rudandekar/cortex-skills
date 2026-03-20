{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_summary_quote_ftss_3', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SUMMARY_QUOTE_FTSS_3',
        'target_table': 'WI_SUMMARY_QUOTE_FTSS_3',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.346616+00:00'
    }
) }}

WITH 

source_wi_summary_quote_ftss_3 AS (
    SELECT
        item_category_descr,
        bk_so_number_int,
        dv_maint_net_local_total_amt,
        dv_allocation_pct,
        sales_motion_cd,
        maint_net_local_ttl_amt_agg
    FROM {{ source('raw', 'wi_summary_quote_ftss_3') }}
),

final AS (
    SELECT
        item_category_descr,
        bk_so_number_int,
        dv_maint_net_local_total_amt,
        dv_allocation_pct,
        sales_motion_cd,
        maint_net_local_ttl_amt_agg
    FROM source_wi_summary_quote_ftss_3
)

SELECT * FROM final