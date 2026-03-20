{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_cst_frst_prt_dtl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_CST_FRST_PRT_DTL',
        'target_table': 'STG_CSF_XXCAS_PRJ_CST_FRST_PRT_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.782998+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_cst_frst_prt_dtl AS (
    SELECT
        partner_id,
        forecast_version_id,
        task_id,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_cst_frst_prt_dtl') }}
),

source_csf_xxcas_prj_cst_frct_prt_dtl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        partner_id,
        forecast_version_id,
        task_id,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by
    FROM {{ source('raw', 'csf_xxcas_prj_cst_frct_prt_dtl') }}
),

transformed_exp_csf_xxcas_prj_cst_frst_prt_dtl AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    partner_id,
    forecast_version_id,
    task_id,
    last_update_login,
    created_by,
    creation_date,
    last_update_date,
    last_updated_by
    FROM source_csf_xxcas_prj_cst_frct_prt_dtl
),

final AS (
    SELECT
        partner_id,
        forecast_version_id,
        task_id,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_cst_frst_prt_dtl
)

SELECT * FROM final