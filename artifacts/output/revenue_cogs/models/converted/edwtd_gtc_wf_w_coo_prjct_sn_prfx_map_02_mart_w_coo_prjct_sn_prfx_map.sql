{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_coo_prjct_sn_prfx_map', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_COO_PRJCT_SN_PRFX_MAP',
        'target_table': 'W_COO_PRJCT_SN_PRFX_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.976233+00:00'
    }
) }}

WITH 

source_st_co_snprefix AS (
    SELECT
        batch_id,
        snprefix_id,
        project_id,
        snprefix,
        coo,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_co_snprefix') }}
),

source_ex_co_snprefix AS (
    SELECT
        batch_id,
        snprefix_id,
        project_id,
        snprefix,
        coo,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        exception_cd,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_co_snprefix') }}
),

source_w_coo_prjct_sn_prfx_map AS (
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
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_coo_prjct_sn_prfx_map') }}
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
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM source_w_coo_prjct_sn_prfx_map
)

SELECT * FROM final