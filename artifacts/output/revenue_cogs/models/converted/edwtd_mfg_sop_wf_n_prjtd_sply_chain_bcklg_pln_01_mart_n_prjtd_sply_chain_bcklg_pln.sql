{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_prjtd_sply_chain_bcklg_pln', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_N_PRJTD_SPLY_CHAIN_BCKLG_PLN',
        'target_table': 'N_PRJTD_SPLY_CHAIN_BCKLG_PLN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.575559+00:00'
    }
) }}

WITH 

source_w_prjtd_sply_chain_bcklg_pln AS (
    SELECT
        bk_product_family_id,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        bk_fiscal_week_start_dt,
        bk_src_publish_dtm,
        dv_src_publish_dt,
        buildable_usd_amt,
        unbuildable_usd_amt,
        unstaged_usd_amt,
        staged_usd_amt,
        total_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_prjtd_sply_chain_bcklg_pln') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        bk_fiscal_week_start_dt,
        bk_src_publish_dtm,
        dv_src_publish_dt,
        buildable_usd_amt,
        unbuildable_usd_amt,
        unstaged_usd_amt,
        staged_usd_amt,
        total_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_prjtd_sply_chain_bcklg_pln
)

SELECT * FROM final