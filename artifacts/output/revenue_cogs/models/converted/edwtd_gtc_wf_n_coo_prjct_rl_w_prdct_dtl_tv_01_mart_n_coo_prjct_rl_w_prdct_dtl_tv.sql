{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_coo_prjct_rl_w_prdct_dtl_tv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_COO_PRJCT_RL_W_PRDCT_DTL_TV',
        'target_table': 'N_COO_PRJCT_RL_W_PRDCT_DTL_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.322920+00:00'
    }
) }}

WITH 

source_n_coo_prjct_rl_w_prdct_dtl AS (
    SELECT
        product_key,
        bk_invalid_country_cd,
        dtl_sts_cd,
        probable_country_cd,
        dtl_create_dt,
        dtl_last_update_dt,
        dtl_last_upd_csco_wrkr_pty_key,
        dtl_created_csco_wrkr_pty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_coo_prjct_rl_w_prdct_dtl') }}
),

source_n_coo_prjct_rl_w_prdct_dtl_tv AS (
    SELECT
        product_key,
        bk_invalid_country_cd,
        dtl_sts_cd,
        probable_country_cd,
        dtl_create_dt,
        dtl_last_update_dt,
        dtl_last_upd_csco_wrkr_pty_key,
        dtl_created_csco_wrkr_pty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'n_coo_prjct_rl_w_prdct_dtl_tv') }}
),

final AS (
    SELECT
        product_key,
        bk_invalid_country_cd,
        dtl_sts_cd,
        probable_country_cd,
        dtl_create_dt,
        dtl_last_update_dt,
        dtl_last_upd_csco_wrkr_pty_key,
        dtl_created_csco_wrkr_pty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM source_n_coo_prjct_rl_w_prdct_dtl_tv
)

SELECT * FROM final