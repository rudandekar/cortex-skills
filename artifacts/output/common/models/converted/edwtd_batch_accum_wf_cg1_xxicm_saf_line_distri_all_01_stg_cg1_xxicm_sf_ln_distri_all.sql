{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxicm_saf_line_distri_all', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXICM_SAF_LINE_DISTRI_ALL',
        'target_table': 'STG_CG1_XXICM_SF_LN_DISTRI_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.876174+00:00'
    }
) }}

WITH 

source_cg1_xxicm_saf_line_distri_all AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        saf_line_distribution_id,
        saf_line_id,
        amount,
        code_combination_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login
    FROM {{ source('raw', 'cg1_xxicm_saf_line_distri_all') }}
),

source_stg_cg1_xxicm_sf_ln_distri_all AS (
    SELECT
        saf_line_distribution_id,
        saf_line_id,
        amount,
        code_combination_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxicm_sf_ln_distri_all') }}
),

transformed_exp_cg1_xxicm_saf_line_distri_all AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    saf_line_distribution_id,
    saf_line_id,
    amount,
    code_combination_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login
    FROM source_stg_cg1_xxicm_sf_ln_distri_all
),

final AS (
    SELECT
        saf_line_distribution_id,
        saf_line_id,
        amount,
        code_combination_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxicm_saf_line_distri_all
)

SELECT * FROM final