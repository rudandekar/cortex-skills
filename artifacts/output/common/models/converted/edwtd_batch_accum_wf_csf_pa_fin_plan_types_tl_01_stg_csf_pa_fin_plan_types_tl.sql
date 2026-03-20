{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_pa_fin_plan_types_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_FIN_PLAN_TYPES_TL',
        'target_table': 'STG_CSF_PA_FIN_PLAN_TYPES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.746857+00:00'
    }
) }}

WITH 

source_stg_csf_pa_fin_plan_types_tl AS (
    SELECT
        fin_plan_type_id,
        name,
        description,
        language_,
        source_lang,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_fin_plan_types_tl') }}
),

source_csf_pa_fin_plan_types_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        fin_plan_type_id,
        name,
        description,
        language,
        source_lang,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_pa_fin_plan_types_tl') }}
),

transformed_exp_csf_pa_fin_plan_types_tl AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    fin_plan_type_id,
    name,
    description,
    language,
    source_lang,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    zd_edition_name,
    zd_sync
    FROM source_csf_pa_fin_plan_types_tl
),

final AS (
    SELECT
        fin_plan_type_id,
        name,
        description,
        language_,
        source_lang,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_fin_plan_types_tl
)

SELECT * FROM final