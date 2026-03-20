{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_audt_pool_rule_res', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_AUDT_POOL_RULE_RES',
        'target_table': 'FF_XXCFI_CB_AUDT_POOL_RULE_RES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.845923+00:00'
    }
) }}

WITH 

source_xxcfi_cb_audit_pool_rule_res AS (
    SELECT
        audit_result_id,
        audit_query_id,
        rule_id,
        specific_name,
        pid_count,
        rule_comment,
        commit_status,
        rule_rank,
        pool_target_flag,
        audit_month,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        specific_id
    FROM {{ source('raw', 'xxcfi_cb_audit_pool_rule_res') }}
),

transformed_exp_ff_xxcfi_cb_audt_pool_rule_res AS (
    SELECT
    audit_result_id,
    audit_query_id,
    rule_id,
    specific_name,
    pid_count,
    rule_comment,
    commit_status,
    rule_rank,
    pool_target_flag,
    audit_month,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    specific_id,
    REG_REPLACE(RULE_COMMENT,'\s+',' ') AS o_rule_comment,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_audit_pool_rule_res
),

final AS (
    SELECT
        audit_result_id,
        audit_query_id,
        rule_id,
        specific_name,
        pid_count,
        rule_comment,
        commit_status,
        rule_rank,
        pool_target_flag,
        audit_month,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        specific_id,
        create_datetime,
        action_code
    FROM transformed_exp_ff_xxcfi_cb_audt_pool_rule_res
)

SELECT * FROM final