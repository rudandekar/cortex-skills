{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sr_workgroup_acts_snapshot', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_SR_WORKGROUP_ACTS_SNAPSHOT',
        'target_table': 'W_SR_WORKGROUP_ACTS_SNAPSHOT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.743935+00:00'
    }
) }}

WITH 

source_sm_sr_workgroup_acts_snapshot AS (
    SELECT
        sr_workgroup_acts_snapshot_key,
        sk_dw_wg_key_int,
        bk_sr_workgroup_cd,
        bk_fscl_cal_cd,
        bk_fscl_mth_num_int,
        bk_fscl_yr_num_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_sr_workgroup_acts_snapshot') }}
),

source_w_sr_workgroup_acts_snapshot AS (
    SELECT
        sr_workgroup_acts_snapshot_key,
        bk_sr_workgroup_cd,
        bk_fscl_cal_cd,
        bk_fscl_mth_num_int,
        bk_fscl_yr_num_int,
        hierarchy_version_num_int,
        active_flg,
        csco_wrkr_prty_key,
        master_theater_name,
        sk_natural_key_int,
        roll_up_theater_name,
        supported_service_line_name,
        supported_service_type_name,
        supported_tech_name,
        theater_name,
        fscl_mth_closed_status_cd,
        sk_dw_wg_key_int,
        sk_rprts_to_object_num_int,
        hierarchy_root_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sr_workgroup_acts_snapshot') }}
),

final AS (
    SELECT
        sr_workgroup_acts_snapshot_key,
        bk_sr_workgroup_cd,
        bk_fscl_cal_cd,
        bk_fscl_mth_num_int,
        bk_fscl_yr_num_int,
        hierarchy_version_num_int,
        active_flg,
        csco_wrkr_prty_key,
        master_theater_name,
        sk_natural_key_int,
        roll_up_theater_name,
        supported_service_line_name,
        supported_service_type_name,
        supported_tech_name,
        theater_name,
        fscl_mth_closed_status_cd,
        sk_dw_wg_key_int,
        sk_rprts_to_object_num_int,
        hierarchy_root_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_sr_workgroup_acts_snapshot
)

SELECT * FROM final