{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xla_transaction_entities_ar', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XLA_TRANSACTION_ENTITIES_AR',
        'target_table': 'EL_XLA_TRANSACTION_ENTITIES_AR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.341988+00:00'
    }
) }}

WITH 

source_st_xla_transaction_entities AS (
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
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xla_transaction_entities') }}
),

final AS (
    SELECT
        global_name,
        entity_id,
        application_id,
        entity_code,
        creation_date,
        source_application_id,
        source_id_int_1,
        transaction_number,
        ledger_id,
        last_update_date,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM source_st_xla_transaction_entities
)

SELECT * FROM final