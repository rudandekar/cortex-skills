{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_coo_prjct_sn_prfx_map_tv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_COO_PRJCT_SN_PRFX_MAP_TV',
        'target_table': 'N_COO_PRJCT_SN_PRFX_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.875972+00:00'
    }
) }}

WITH 

source_n_coo_prjct_sn_prfx_map AS (
    SELECT
        bk_coo_prjct_name,
        bk_coo_country_cd,
        bk_sn_prefix_num_cd,
        sn_map_create_dtm,
        dv_sn_map_create_dt,
        sn_map_last_update_dtm,
        dv_sn_map_last_update_dt,
        sn_map_created_auto_test_flg,
        sn_map_updated_auto_test_flg,
        source_deleted_flg,
        sn_map_lt_upd_csco_wkr_pty_key,
        sn_map_create_csco_wkr_pty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_coo_prjct_sn_prfx_map') }}
),

source_n_coo_prjct_sn_prfx_map_tv AS (
    SELECT
        bk_coo_prjct_name,
        bk_coo_country_cd,
        bk_sn_prefix_num_cd,
        sn_map_create_dtm,
        dv_sn_map_create_dt,
        sn_map_last_update_dtm,
        dv_sn_map_last_update_dt,
        sn_map_created_auto_test_flg,
        sn_map_updated_auto_test_flg,
        source_deleted_flg,
        sn_map_lt_upd_csco_wkr_pty_key,
        sn_map_create_csco_wkr_pty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM {{ source('raw', 'n_coo_prjct_sn_prfx_map_tv') }}
),

final AS (
    SELECT
        bk_coo_prjct_name,
        bk_coo_country_cd,
        bk_sn_prefix_num_cd,
        sn_map_create_dtm,
        dv_sn_map_create_dt,
        sn_map_last_update_dtm,
        dv_sn_map_last_update_dt,
        sn_map_created_auto_test_flg,
        sn_map_updated_auto_test_flg,
        source_deleted_flg,
        sn_map_lt_upd_csco_wkr_pty_key,
        sn_map_create_csco_wkr_pty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_coo_prjct_sn_prfx_map_tv
)

SELECT * FROM final