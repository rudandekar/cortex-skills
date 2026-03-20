{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_accounting_rule_tv_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_N_AR_ACCOUNTING_RULE_TV_OOD',
        'target_table': 'N_AR_ACCOUNTING_RULE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.017567+00:00'
    }
) }}

WITH 

source_w_ar_accounting_rule AS (
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
    FROM {{ source('raw', 'w_ar_accounting_rule') }}
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
        edw_update_datetime
    FROM source_w_ar_accounting_rule
)

SELECT * FROM final