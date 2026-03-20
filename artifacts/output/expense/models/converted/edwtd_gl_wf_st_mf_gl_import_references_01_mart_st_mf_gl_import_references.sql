{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_gl_import_references', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_GL_IMPORT_REFERENCES',
        'target_table': 'ST_MF_GL_IMPORT_REFERENCES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.711374+00:00'
    }
) }}

WITH 

source_ff_mf_gl_import_references AS (
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
    FROM {{ source('raw', 'ff_mf_gl_import_references') }}
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
    FROM source_ff_mf_gl_import_references
)

SELECT * FROM final