{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_om_cfi_rev_adj_trx_all', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_OM_CFI_REV_ADJ_TRX_ALL',
        'target_table': 'EL_OM_CFI_REV_ADJ_TRX_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.101541+00:00'
    }
) }}

WITH 

source_el_om_cfi_rev_adj_trx_all AS (
    SELECT
        global_name,
        rev_adj_trx_id,
        adj_type_id,
        adjustment_revenue_usd,
        adjustment_cogs_usd,
        period_year,
        period_num
    FROM {{ source('raw', 'el_om_cfi_rev_adj_trx_all') }}
),

source_st_om_cfi_rev_adj_trx_all AS (
    SELECT
        batch_id,
        global_name,
        rev_adj_trx_id,
        adj_type_id,
        adj_source_type,
        source_header_id,
        source_line_id,
        currency_code,
        adjustment_revenue,
        adjustment_revenue_usd,
        adjustment_cogs_usd,
        period_year,
        period_num,
        rev_summary_id,
        org_id,
        set_of_books_id,
        csm_flag,
        transaction_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cfi_rev_adj_trx_all') }}
),

final AS (
    SELECT
        global_name,
        rev_adj_trx_id,
        adj_type_id,
        adjustment_revenue_usd,
        adjustment_cogs_usd,
        period_year,
        period_num
    FROM source_st_om_cfi_rev_adj_trx_all
)

SELECT * FROM final