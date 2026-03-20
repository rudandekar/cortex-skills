{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cp_account_rules', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CP_ACCOUNT_RULES',
        'target_table': 'EL_CP_ACCOUNT_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.190737+00:00'
    }
) }}

WITH 

source_el_cp_account_rules AS (
    SELECT
        rule_id,
        rule_name,
        source_id,
        entity_type,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code,
        transaction_type
    FROM {{ source('raw', 'el_cp_account_rules') }}
),

final AS (
    SELECT
        rule_id,
        rule_name,
        source_id,
        entity_type,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code,
        transaction_type
    FROM source_el_cp_account_rules
)

SELECT * FROM final