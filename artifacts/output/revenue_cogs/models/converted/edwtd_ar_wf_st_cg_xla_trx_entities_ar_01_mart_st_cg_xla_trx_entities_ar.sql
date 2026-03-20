{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg_xla_trx_entities_ar', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CG_XLA_TRX_ENTITIES_AR',
        'target_table': 'ST_CG_XLA_TRX_ENTITIES_AR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.109265+00:00'
    }
) }}

WITH 

source_cg1_xla_transaction_entities AS (
    SELECT
        entity_id,
        application_id,
        legal_entity_id,
        entity_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        source_id_int_1,
        source_id_char_1,
        security_id_int_1,
        security_id_int_2,
        security_id_int_3,
        security_id_char_1,
        security_id_char_2,
        security_id_char_3,
        source_id_int_2,
        source_id_char_2,
        source_id_int_3,
        source_id_char_3,
        source_id_int_4,
        source_id_char_4,
        transaction_number,
        ledger_id,
        valuation_method,
        source_application_id,
        upg_batch_id,
        upg_source_application_id,
        upg_valid_flag,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_xla_transaction_entities') }}
),

final AS (
    SELECT
        batch_id,
        entity_id,
        application_id,
        legal_entity_id,
        entity_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        source_id_int_1,
        source_id_char_1,
        security_id_int_1,
        security_id_int_2,
        security_id_int_3,
        security_id_char_1,
        security_id_char_2,
        security_id_char_3,
        source_id_int_2,
        source_id_char_2,
        source_id_int_3,
        source_id_char_3,
        source_id_int_4,
        source_id_char_4,
        transaction_number,
        ledger_id,
        valuation_method,
        source_application_id,
        upg_batch_id,
        upg_source_application_id,
        upg_valid_flag,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM source_cg1_xla_transaction_entities
)

SELECT * FROM final