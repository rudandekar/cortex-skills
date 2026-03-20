{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_audit_clsfn_cmnts', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_AUDIT_CLSFN_CMNTS',
        'target_table': 'FF_XXCFI_CB_AUDIT_CLSFN_CMNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.951260+00:00'
    }
) }}

WITH 

source_xxcfi_cb_audit_clsfn_comments AS (
    SELECT
        audit_comment_id,
        audit_query_id,
        pid,
        auditor_comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date
    FROM {{ source('raw', 'xxcfi_cb_audit_clsfn_comments') }}
),

transformed_exp_ff_xxcfi_cb_audit_clsfn_cmnts AS (
    SELECT
    audit_comment_id,
    audit_query_id,
    pid,
    auditor_comments,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_audit_clsfn_comments
),

final AS (
    SELECT
        audit_comment_id,
        audit_query_id,
        pid,
        auditor_comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_ff_xxcfi_cb_audit_clsfn_cmnts
)

SELECT * FROM final