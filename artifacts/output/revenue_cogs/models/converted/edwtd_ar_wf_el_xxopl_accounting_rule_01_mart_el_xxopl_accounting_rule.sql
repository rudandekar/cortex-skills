{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxopl_accounting_rule', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXOPL_ACCOUNTING_RULE',
        'target_table': 'EL_XXOPL_ACCOUNTING_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.139707+00:00'
    }
) }}

WITH 

source_el_xxopl_accounting_rule AS (
    SELECT
        subscription_ref_id,
        subscription_code,
        product_key,
        dv_product_key,
        line_ref_number,
        parent_line_ref_number,
        accounting_rule,
        accounting_rule_duration,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_xxopl_accounting_rule') }}
),

final AS (
    SELECT
        subscription_ref_id,
        subscription_code,
        product_key,
        dv_product_key,
        line_ref_number,
        parent_line_ref_number,
        accounting_rule,
        accounting_rule_duration,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_el_xxopl_accounting_rule
)

SELECT * FROM final