{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_audit_query', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_AUDIT_QUERY',
        'target_table': 'ST_XXCFI_CB_AUDIT_QUERY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.266715+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_audit_query AS (
    SELECT
        audit_comments,
        audit_commited,
        audit_country_group,
        audit_fiscal_year,
        audit_month,
        audit_name,
        audit_owner,
        audit_query_id,
        audit_record_count,
        audit_record_count_type,
        audit_status,
        audit_type,
        audit_year,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_audit_query') }}
),

final AS (
    SELECT
        audit_comments,
        audit_commited,
        audit_country_group,
        audit_fiscal_year,
        audit_month,
        audit_name,
        audit_owner,
        audit_query_id,
        audit_record_count,
        audit_record_count_type,
        audit_status,
        audit_type,
        audit_year,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_audit_query
)

SELECT * FROM final