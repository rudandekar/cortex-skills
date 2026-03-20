{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_rules', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_RULES',
        'target_table': 'FF_XXCFI_CB_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.758705+00:00'
    }
) }}

WITH 

source_xxcfi_cb_rules AS (
    SELECT
        rule_id,
        specific_id,
        specific_name,
        override_key_in,
        active_flag,
        start_date,
        end_date,
        override_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        audit_flag
    FROM {{ source('raw', 'xxcfi_cb_rules') }}
),

transformed_exp_ff_xxcfi_cb_rules AS (
    SELECT
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
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_rules
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
    FROM transformed_exp_ff_xxcfi_cb_rules
)

SELECT * FROM final