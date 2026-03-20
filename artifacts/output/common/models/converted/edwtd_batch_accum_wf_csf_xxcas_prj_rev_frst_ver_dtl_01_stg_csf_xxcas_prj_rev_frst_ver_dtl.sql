{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_rev_frst_ver_dtl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_REV_FRST_VER_DTL',
        'target_table': 'STG_CSF_XXCAS_PRJ_REV_FRST_VER_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.848441+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_rev_frst_ver_dtl AS (
    SELECT
        budget_ver_line_id,
        budget_version_id,
        forecast_date,
        forecast_amount,
        projfunc_forecast_amount,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_rev_frst_ver_dtl') }}
),

source_csf_xxcas_prj_rev_frct_ver_dtl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        budget_ver_line_id,
        budget_version_id,
        forecast_date,
        forecast_amount,
        projfunc_forecast_amount,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        last_update_login
    FROM {{ source('raw', 'csf_xxcas_prj_rev_frct_ver_dtl') }}
),

transformed_exp_csf_xxcas_prj_rev_frst_ver_dtl AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    budget_ver_line_id,
    budget_version_id,
    forecast_date,
    forecast_amount,
    projfunc_forecast_amount,
    created_by,
    creation_date,
    last_update_date,
    last_updated_by,
    last_update_login
    FROM source_csf_xxcas_prj_rev_frct_ver_dtl
),

final AS (
    SELECT
        budget_ver_line_id,
        budget_version_id,
        forecast_date,
        forecast_amount,
        projfunc_forecast_amount,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_rev_frst_ver_dtl
)

SELECT * FROM final