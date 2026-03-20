{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ic_accounting_rule_type_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_IC_ACCOUNTING_RULE_TYPE_STG23NF',
        'target_table': 'N_IC_ACCOUNTING_RULE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.362490+00:00'
    }
) }}

WITH 

source_n_ic_accounting_rule_type AS (
    SELECT
        bk_ic_accounting_rule_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_ic_accounting_rule_type') }}
),

final AS (
    SELECT
        bk_ic_accounting_rule_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_ic_accounting_rule_type
)

SELECT * FROM final