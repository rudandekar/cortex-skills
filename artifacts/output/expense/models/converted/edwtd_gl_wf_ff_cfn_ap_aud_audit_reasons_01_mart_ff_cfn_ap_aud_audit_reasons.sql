{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cfn_ap_aud_audit_reasons', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_CFN_AP_AUD_AUDIT_REASONS',
        'target_table': 'FF_CFN_AP_AUD_AUDIT_REASONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.026133+00:00'
    }
) }}

WITH 

source_cfn_ap_aud_audit_reasons AS (
    SELECT
        audit_reason_id,
        report_header_id,
        audit_reason_code,
        creation_date,
        created_by,
        last_update_login,
        last_update_date,
        last_updated_by,
        global_name,
        ges_update_date
    FROM {{ source('raw', 'cfn_ap_aud_audit_reasons') }}
),

transformed_exp_cfn_ap_aud_audit_reasons AS (
    SELECT
    audit_reason_id,
    report_header_id,
    audit_reason_code,
    creation_date,
    created_by,
    last_update_login,
    last_update_date,
    last_updated_by,
    global_name,
    ges_update_date,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_cretae_datetime,
    'I' AS o_action_code
    FROM source_cfn_ap_aud_audit_reasons
),

final AS (
    SELECT
        batch_id,
        audit_reason_id,
        report_header_id,
        audit_reason_code,
        creation_date,
        created_by,
        last_update_login,
        last_update_date,
        last_updated_by,
        global_name,
        ges_update_date,
        action_code,
        create_datetime
    FROM transformed_exp_cfn_ap_aud_audit_reasons
)

SELECT * FROM final