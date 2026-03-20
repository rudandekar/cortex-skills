{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_gl_import_references', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_GL_IMPORT_REFERENCES',
        'target_table': 'FF_MF_GL_IMPORT_REFERENCES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.612627+00:00'
    }
) }}

WITH 

source_mf_gl_import_references AS (
    SELECT
        created_by,
        creation_date,
        ges_pk_id,
        ges_update_date,
        global_name,
        gl_sl_link_id,
        gl_sl_link_table,
        je_batch_id,
        je_header_id,
        je_line_num,
        last_updated_by,
        last_update_date,
        last_update_login,
        reference_1,
        reference_10,
        reference_2,
        reference_3,
        reference_4,
        reference_5,
        reference_6,
        reference_7,
        reference_8,
        reference_9,
        subledger_doc_sequence_id,
        subledger_doc_sequence_value
    FROM {{ source('raw', 'mf_gl_import_references') }}
),

transformed_exp_mf_gl_import_references AS (
    SELECT
    created_by,
    creation_date,
    ges_pk_id,
    ges_update_date,
    global_name,
    gl_sl_link_id,
    gl_sl_link_table,
    je_batch_id,
    je_header_id,
    je_line_num,
    last_updated_by,
    last_update_date,
    last_update_login,
    reference_1,
    reference_10,
    reference_2,
    reference_3,
    reference_4,
    reference_5,
    reference_6,
    reference_7,
    reference_8,
    reference_9,
    subledger_doc_sequence_id,
    subledger_doc_sequence_value,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS creat_datetime,
    'I' AS action_code
    FROM source_mf_gl_import_references
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        ges_pk_id,
        ges_update_date,
        global_name,
        gl_sl_link_id,
        gl_sl_link_table,
        je_batch_id,
        je_header_id,
        je_line_num,
        last_updated_by,
        last_update_date,
        last_update_login,
        reference_1,
        reference_10,
        reference_2,
        reference_3,
        reference_4,
        reference_5,
        reference_6,
        reference_7,
        reference_8,
        reference_9,
        subledger_doc_sequence_id,
        subledger_doc_sequence_value,
        create_datetime,
        action_code
    FROM transformed_exp_mf_gl_import_references
)

SELECT * FROM final