{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_rules', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_RULES',
        'target_table': 'ST_XXCFI_CB_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.761272+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_rules AS (
    SELECT
        batch_id,
        rule_id,
        specific_id,
        specific_name,
        override_key_in,
        start_date,
        end_date,
        override_flag,
        active_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        audit_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_rules') }}
),

final AS (
    SELECT
        batch_id,
        rule_id,
        specific_id,
        specific_name,
        override_key_in,
        start_date,
        end_date,
        override_flag,
        active_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        audit_flag,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_rules
)

SELECT * FROM final