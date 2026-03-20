{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_audit_target_results', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_AUDIT_TARGET_RESULTS',
        'target_table': 'ST_XXCFI_CB_AUDIT_TRGT_RESULTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.993710+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_audit_trgt_results AS (
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
        is_correct_specific,
        impacting_us_hts,
        impacting_cg_hts,
        new_specific,
        auditor_comment,
        reason_mis_classification,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        erp_specific_id,
        custom_specific_id,
        new_specific_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_audit_trgt_results') }}
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
        is_correct_specific,
        impacting_us_hts,
        impacting_cg_hts,
        new_specific,
        auditor_comment,
        reason_mis_classification,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        erp_specific_id,
        custom_specific_id,
        new_specific_id,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_audit_trgt_results
)

SELECT * FROM final