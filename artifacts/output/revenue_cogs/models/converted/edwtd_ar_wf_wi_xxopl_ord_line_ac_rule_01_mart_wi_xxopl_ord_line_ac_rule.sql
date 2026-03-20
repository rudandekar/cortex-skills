{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_xxopl_ord_line_ac_rule', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_XXOPL_ORD_LINE_AC_RULE',
        'target_table': 'WI_XXOPL_ORD_LINE_AC_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.317575+00:00'
    }
) }}

WITH 

source_wi_xxopl_ord_line_ac_rule AS (
    SELECT
        header_id,
        line_id,
        ordered_item,
        item_type_code,
        subscription_ref_id,
        subscription_id,
        contract_start_date,
        contract_end_date,
        line_ref_number,
        accounting_rule,
        accounting_rule_duration,
        source_commit_time
    FROM {{ source('raw', 'wi_xxopl_ord_line_ac_rule') }}
),

final AS (
    SELECT
        header_id,
        line_id,
        ordered_item,
        item_type_code,
        subscription_ref_id,
        subscription_id,
        contract_start_date,
        contract_end_date,
        line_ref_number,
        accounting_rule,
        accounting_rule_duration,
        source_commit_time
    FROM source_wi_xxopl_ord_line_ac_rule
)

SELECT * FROM final