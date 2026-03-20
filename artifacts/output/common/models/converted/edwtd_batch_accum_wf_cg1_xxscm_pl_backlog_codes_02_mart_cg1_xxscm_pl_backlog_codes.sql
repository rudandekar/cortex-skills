{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxscm_pl_backlog_codes', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXSCM_PL_BACKLOG_CODES',
        'target_table': 'CG1_XXSCM_PL_BACKLOG_CODES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.971651+00:00'
    }
) }}

WITH 

source_stg_cg1_xxscm_pl_backlog_codes AS (
    SELECT
        type1,
        column1,
        column2,
        column3,
        column4,
        column5,
        codes_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        disabled1,
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_cg1_xxscm_pl_backlog_codes') }}
),

source_cg1_xxscm_pl_backlog_codes AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        type,
        column1,
        column2,
        column3,
        column4,
        column5,
        codes_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        disabled
    FROM {{ source('raw', 'cg1_xxscm_pl_backlog_codes') }}
),

transformed_exp_cg1_xxscm_pl_backlog_codes AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    type,
    column1,
    column2,
    column3,
    column4,
    column5,
    codes_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    disabled
    FROM source_cg1_xxscm_pl_backlog_codes
),

final AS (
    SELECT
        type1,
        column1,
        column2,
        column3,
        column4,
        column5,
        codes_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        disabled1,
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_cg1_xxscm_pl_backlog_codes
)

SELECT * FROM final