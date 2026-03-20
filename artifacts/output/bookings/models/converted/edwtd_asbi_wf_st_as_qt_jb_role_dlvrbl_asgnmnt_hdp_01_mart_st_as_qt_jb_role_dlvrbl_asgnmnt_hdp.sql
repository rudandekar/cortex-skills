{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_qt_jb_role_dlvrbl_asgnmnt_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QT_JB_ROLE_DLVRBL_ASGNMNT_HDP',
        'target_table': 'ST_AS_QT_JB_ROLE_DLVRBL_ASGNMNT_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.898911+00:00'
    }
) }}

WITH 

source_ff_as_qt_jb_role_dlvrbl_asgnmnt_hdp AS (
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
        ss_cd
    FROM {{ source('raw', 'ff_as_qt_jb_role_dlvrbl_asgnmnt_hdp') }}
),

transformed_exptrans1 AS (
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
    ss_cd,
    CURRENT_TIMESTAMP() AS created_dtm
    FROM source_ff_as_qt_jb_role_dlvrbl_asgnmnt_hdp
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
        bk_ss_cd,
        created_dtm
    FROM transformed_exptrans1
)

SELECT * FROM final