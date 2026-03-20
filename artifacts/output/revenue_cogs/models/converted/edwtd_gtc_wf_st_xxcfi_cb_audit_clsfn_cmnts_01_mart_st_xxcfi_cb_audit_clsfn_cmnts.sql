{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_audit_clsfn_cmnts', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_AUDIT_CLSFN_CMNTS',
        'target_table': 'ST_XXCFI_CB_AUDIT_CLSFN_CMNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.644212+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_audit_clsfn_cmnts AS (
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
    FROM {{ source('raw', 'ff_xxcfi_cb_audit_clsfn_cmnts') }}
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
    FROM source_ff_xxcfi_cb_audit_clsfn_cmnts
)

SELECT * FROM final