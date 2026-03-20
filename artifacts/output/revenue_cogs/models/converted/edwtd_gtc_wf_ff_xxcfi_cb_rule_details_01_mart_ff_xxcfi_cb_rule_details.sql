{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_rule_details', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_RULE_DETAILS',
        'target_table': 'FF_XXCFI_CB_RULE_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.351083+00:00'
    }
) }}

WITH 

source_xxcfi_cb_rule_details AS (
    SELECT
        rule_details_id,
        rule_id,
        country_group_id,
        country_group_code,
        hts_code,
        start_date,
        end_date,
        active_flag,
        rule_status,
        processed_flag,
        rule_reversed,
        created_by,
        created_date,
        modified_by,
        modified_date
    FROM {{ source('raw', 'xxcfi_cb_rule_details') }}
),

transformed_ex_ff_xxcfi_cb_rule_details AS (
    SELECT
    rule_details_id,
    rule_id,
    country_group_id,
    country_group_code,
    hts_code,
    start_date,
    end_date,
    active_flag,
    rule_status,
    processed_flag,
    rule_reversed,
    created_by,
    created_date,
    modified_by,
    modified_date,
    'BatchId' AS batch_id,
    'I' AS action_cd,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_xxcfi_cb_rule_details
),

final AS (
    SELECT
        rule_details_id,
        rule_id,
        country_group_id,
        country_group_code,
        hts_code,
        start_date,
        end_date,
        active_flag,
        rule_status,
        processed_flag,
        rule_reversed,
        created_by,
        created_date,
        modified_by,
        modified_date,
        batch_id,
        action_cd,
        create_datetime
    FROM transformed_ex_ff_xxcfi_cb_rule_details
)

SELECT * FROM final