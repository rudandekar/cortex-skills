{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ar_accounting_rule_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_WK_AR_ACCOUNTING_RULE_OOD',
        'target_table': 'W_AR_ACCOUNTING_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.470978+00:00'
    }
) }}

WITH 

source_st_ood_ra_rules AS (
    SELECT
        description,
        name,
        rule_id,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ood_ra_rules') }}
),

transformed_exp_w_ar_accounting_rule_ood AS (
    SELECT
    name,
    description,
    rule_id,
    ss_code,
    start_tv_date,
    end_tv_date,
    action_code,
    rank_index,
    dml_type,
    create_datetime
    FROM source_st_ood_ra_rules
),

final AS (
    SELECT
        bk_accounting_rule_name,
        start_tv_date,
        end_tv_date,
        ar_accounting_rule_description,
        sk_rule_id_int,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM transformed_exp_w_ar_accounting_rule_ood
)

SELECT * FROM final