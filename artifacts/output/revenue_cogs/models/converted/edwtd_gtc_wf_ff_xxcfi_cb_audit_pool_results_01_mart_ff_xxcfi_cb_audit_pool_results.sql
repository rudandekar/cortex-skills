{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_audit_pool_results', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_AUDIT_POOL_RESULTS',
        'target_table': 'FF_XXCFI_CB_AUDIT_POOL_RESULTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.268620+00:00'
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

source_xxcfi_cb_audit_pool_results AS (
    SELECT
        audit_result_id,
        audit_query_id,
        audit_name,
        audit_owner,
        audit_month,
        audit_year,
        pid,
        description,
        custom_specific,
        erp_specific,
        source,
        pt_cc,
        country_group,
        us_hts,
        cg_hts,
        cg_duty_rate,
        requestor,
        trained_flag,
        commit_status,
        audit_status,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        erp_specific_id,
        custom_specific_id
    FROM {{ source('raw', 'xxcfi_cb_audit_pool_results') }}
),

transformed_exp_ff_xxcfi_cb_audit_pool_results AS (
    SELECT
    audit_result_id,
    audit_query_id,
    audit_name,
    audit_owner,
    audit_month,
    audit_year,
    pid,
    description,
    custom_specific,
    erp_specific,
    source,
    pt_cc,
    country_group,
    us_hts,
    cg_hts,
    cg_duty_rate,
    requestor,
    trained_flag,
    commit_status,
    audit_status,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    erp_specific_id,
    custom_specific_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_audit_pool_results
),

final AS (
    SELECT
        audit_result_id,
        audit_query_id,
        audit_name,
        audit_owner,
        audit_month,
        audit_year,
        pid,
        description,
        custom_specific,
        erp_specific,
        source,
        pt_cc,
        country_group,
        us_hts,
        cg_hts,
        cg_duty_rate,
        requestor,
        trained_flag,
        commit_status,
        audit_status,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        erp_specific_id,
        custom_specific_id,
        create_datetime,
        action_code
    FROM transformed_exp_ff_xxcfi_cb_audit_pool_results
)

SELECT * FROM final