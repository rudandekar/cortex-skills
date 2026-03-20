{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_audit_query', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_AUDIT_QUERY',
        'target_table': 'FF_XXCFI_CB_AUDIT_QUERY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.419722+00:00'
    }
) }}

WITH 

source_xxcfi_cb_audit_query AS (
    SELECT
        audit_query_id,
        audit_name,
        audit_owner,
        audit_type,
        audit_country_group,
        audit_record_count_type,
        audit_record_count,
        audit_month,
        audit_year,
        audit_status,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        audit_comments,
        audit_commited,
        audit_fiscal_year
    FROM {{ source('raw', 'xxcfi_cb_audit_query') }}
),

transformed_exptrans AS (
    SELECT
    audit_query_id,
    audit_name,
    audit_owner,
    audit_type,
    audit_country_group,
    audit_record_count_type,
    audit_record_count,
    audit_month,
    audit_year,
    audit_status,
    created_by,
    creation_date,
    last_updated_by,
    last_updated_date,
    audit_comments,
    audit_commited,
    audit_fiscal_year,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_cd
    FROM source_xxcfi_cb_audit_query
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
    FROM transformed_exptrans
)

SELECT * FROM final