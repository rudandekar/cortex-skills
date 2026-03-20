{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_adt_rul_cg_hts_res', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_ADT_RUL_CG_HTS_RES',
        'target_table': 'FF_XXCFI_CB_ADT_RUL_CG_HTS_RES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.118347+00:00'
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

source_xxcfi_cb_audit_rule_cg_hts_res AS (
    SELECT
        audit_result_id,
        audit_query_id,
        audit_month,
        audit_year,
        audit_fiscal_year,
        rule_id,
        specific_name,
        pid_count,
        country_group_code,
        hts_code,
        duty_rate,
        score,
        rule_cg_comment,
        pool_target_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        new_hts_code,
        audit_performed,
        specific_id
    FROM {{ source('raw', 'xxcfi_cb_audit_rule_cg_hts_res') }}
),

transformed_exp_ff_xxcfi_cb_adt_rul_cg_hts_res AS (
    SELECT
    audit_result_id,
    audit_query_id,
    audit_month,
    audit_year,
    audit_fiscal_year,
    rule_id,
    specific_name,
    pid_count,
    country_group_code,
    hts_code,
    duty_rate,
    score,
    rule_cg_comment,
    pool_target_flag,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    new_hts_code,
    audit_performed,
    specific_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_audit_rule_cg_hts_res
),

final AS (
    SELECT
        audit_result_id,
        audit_query_id,
        audit_month,
        audit_year,
        audit_fiscal_year,
        rule_id,
        specific_name,
        pid_count,
        country_group_code,
        hts_code,
        duty_rate,
        score,
        rule_cg_comment,
        pool_target_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        new_hts_code,
        audit_performed,
        specific_id,
        create_datetime,
        action_code
    FROM transformed_exp_ff_xxcfi_cb_adt_rul_cg_hts_res
)

SELECT * FROM final