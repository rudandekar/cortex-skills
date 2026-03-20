{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_oe_trans_types_tl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_OE_TRANS_TYPES_TL',
        'target_table': 'ST_CG1_OE_TRANS_TYPES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.412702+00:00'
    }
) }}

WITH 

source_cg1_oe_trans_types_tl AS (
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
    FROM {{ source('raw', 'cg1_oe_trans_types_tl') }}
),

final AS (
    SELECT
        transaction_type_id,
        language_1,
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
        source_commit_time,
        global_name
    FROM source_cg1_oe_trans_types_tl
)

SELECT * FROM final