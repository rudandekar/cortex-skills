{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_qt_jb_role_dlvrbl_asgnmnt_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_AS_QT_JB_ROLE_DLVRBL_ASGNMNT_SEP',
        'target_table': 'W_AS_QT_JB_RL_DLV_ASGNMNT_SEP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.727724+00:00'
    }
) }}

WITH 

source_w_as_qt_jb_rl_dlv_asgnmnt_sep AS (
    SELECT
        bk_as_quote_rsrc_job_role_name,
        bk_deliverable_name,
        bk_delivery_group_name,
        bk_resource_type_cd,
        bk_job_role_grade_lvl_cd,
        work_hrs_pct,
        trvl_hrs_pct,
        dlvry_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_as_qt_jb_rl_dlv_asgnmnt_sep') }}
),

final AS (
    SELECT
        bk_as_quote_rsrc_job_role_name,
        bk_deliverable_name,
        bk_delivery_group_name,
        bk_resource_type_cd,
        bk_job_role_grade_lvl_cd,
        work_hrs_pct,
        trvl_hrs_pct,
        dlvry_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        action_code,
        dml_type
    FROM source_w_as_qt_jb_rl_dlv_asgnmnt_sep
)

SELECT * FROM final