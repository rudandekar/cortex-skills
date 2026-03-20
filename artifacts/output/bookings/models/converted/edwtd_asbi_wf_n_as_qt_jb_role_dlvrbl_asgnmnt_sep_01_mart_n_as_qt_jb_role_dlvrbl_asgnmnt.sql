{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_qt_jb_role_dlvrbl_asgnmnt_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_QT_JB_ROLE_DLVRBL_ASGNMNT_SEP',
        'target_table': 'N_AS_QT_JB_ROLE_DLVRBL_ASGNMNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.988090+00:00'
    }
) }}

WITH 

source_n_as_qt_jb_role_dlvrbl_asgnmnt AS (
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
        edw_update_user
    FROM {{ source('raw', 'n_as_qt_jb_role_dlvrbl_asgnmnt') }}
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
        bk_ss_cd
    FROM source_n_as_qt_jb_role_dlvrbl_asgnmnt
)

SELECT * FROM final