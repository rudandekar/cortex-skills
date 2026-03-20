{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cfn_ap_aud_audit_reasons', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CFN_AP_AUD_AUDIT_REASONS',
        'target_table': 'ST_CFN_AP_AUD_AUDIT_REASONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.947000+00:00'
    }
) }}

WITH 

source_ff_cfn_ap_aud_audit_reasons AS (
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
    FROM {{ source('raw', 'ff_cfn_ap_aud_audit_reasons') }}
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
    FROM source_ff_cfn_ap_aud_audit_reasons
)

SELECT * FROM final