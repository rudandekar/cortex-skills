{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_oe_trans_types_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_OE_TRANS_TYPES_TL',
        'target_table': 'STG_CG1_OE_TRANS_TYPES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.666390+00:00'
    }
) }}

WITH 

source_stg_cg1_oe_trans_types_tl AS (
    SELECT
        transaction_type_id,
        name,
        creation_date,
        description,
        last_updated_by,
        last_update_date,
        last_update_login,
        program_application_id,
        program_id,
        request_id,
        source_lang,
        created_by,
        language_1,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_oe_trans_types_tl') }}
),

source_cg1_oe_transaction_types_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        transaction_type_id,
        language,
        source_lang,
        name,
        description,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        program_application_id,
        program_id,
        request_id
    FROM {{ source('raw', 'cg1_oe_transaction_types_tl') }}
),

transformed_exp_cg1_oe_trans_types_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    transaction_type_id,
    language,
    source_lang,
    name,
    description,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    request_id
    FROM source_cg1_oe_transaction_types_tl
),

final AS (
    SELECT
        transaction_type_id,
        name,
        creation_date,
        description,
        last_updated_by,
        last_update_date,
        last_update_login,
        program_application_id,
        program_id,
        request_id,
        source_lang,
        created_by,
        language_1,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_oe_trans_types_tl
)

SELECT * FROM final