{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sw_pos_rules_tags', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_SW_POS_RULES_TAGS',
        'target_table': 'WI_SW_POS_RULES_TAGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.294517+00:00'
    }
) }}

WITH 

source_wi_sw_pos_rules_tags AS (
    SELECT
        pos_transaction_id_int,
        offer_attribution_id_int,
        sales_order_line_key,
        sales_motion_cd,
        reason_code,
        process_flag,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule,
        renewal_gap_days,
        dv_allocation_pct
    FROM {{ source('raw', 'wi_sw_pos_rules_tags') }}
),

final AS (
    SELECT
        pos_transaction_id_int,
        offer_attribution_id_int,
        sales_order_line_key,
        sales_motion_cd,
        reason_code,
        process_flag,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule,
        renewal_gap_days,
        dv_allocation_pct
    FROM source_wi_sw_pos_rules_tags
)

SELECT * FROM final