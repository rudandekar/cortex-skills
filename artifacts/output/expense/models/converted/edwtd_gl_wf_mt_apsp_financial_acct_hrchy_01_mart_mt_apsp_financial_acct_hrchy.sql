{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_apsp_financial_acct_hrchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_MT_APSP_FINANCIAL_ACCT_HRCHY',
        'target_table': 'MT_APSP_FINANCIAL_ACCT_HRCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.608437+00:00'
    }
) }}

WITH 

source_mt_apsp_financial_acct_hrchy AS (
    SELECT
        bk_financial_account_code,
        level01_acct_group_id,
        level02_acct_group_id,
        level03_acct_group_id,
        level04_acct_group_id,
        level05_acct_group_id,
        level06_acct_group_id,
        level07_acct_group_id,
        level08_acct_group_id,
        level09_acct_group_id,
        level10_acct_group_id,
        level11_acct_group_id,
        level12_acct_group_id,
        level13_acct_group_id,
        level14_acct_group_id,
        level15_acct_group_id,
        dv_category_type,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'mt_apsp_financial_acct_hrchy') }}
),

final AS (
    SELECT
        bk_financial_account_code,
        level01_acct_group_id,
        level02_acct_group_id,
        level03_acct_group_id,
        level04_acct_group_id,
        level05_acct_group_id,
        level06_acct_group_id,
        level07_acct_group_id,
        level08_acct_group_id,
        level09_acct_group_id,
        level10_acct_group_id,
        level11_acct_group_id,
        level12_acct_group_id,
        level13_acct_group_id,
        level14_acct_group_id,
        level15_acct_group_id,
        dv_category_type,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_mt_apsp_financial_acct_hrchy
)

SELECT * FROM final