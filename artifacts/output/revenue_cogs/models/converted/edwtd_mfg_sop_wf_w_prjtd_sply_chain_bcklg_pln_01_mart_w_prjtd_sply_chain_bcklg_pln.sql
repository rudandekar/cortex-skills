{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_prjtd_sply_chain_bcklg_pln', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_W_PRJTD_SPLY_CHAIN_BCKLG_PLN',
        'target_table': 'W_PRJTD_SPLY_CHAIN_BCKLG_PLN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.498023+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_crp_backlog AS (
    SELECT
        product_family,
        dimension,
        fiscal_week_start_date,
        buildable,
        unbuildable,
        unstaged,
        staged,
        total,
        creation_date
    FROM {{ source('raw', 'st_xxcmf_sp_crp_backlog') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxcmf_sp_crp_backlog
)

SELECT * FROM final