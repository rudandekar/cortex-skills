{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cisco_subscr_fbs_data_v_rpo', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CISCO_SUBSCR_FBS_DATA_V_RPO',
        'target_table': 'ST_CISCO_SUBSCR_FBS_DATA_V_RPO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.012261+00:00'
    }
) }}

WITH 

source_ff_cisco_subscr_fbs_data_v_rpo AS (
    SELECT
        subscription_ref_id,
        subscription_id,
        subscription_status,
        item_id,
        ccw_order_line_id,
        line_status,
        billing_model,
        billing_preference,
        order_type,
        offer_type,
        previous_ccw_order_line_id,
        item_action_detail,
        bill_date,
        charge_cycle_start,
        charge_cycle_end,
        currency_code,
        item_total,
        billing_source,
        last_modified_date
    FROM {{ source('raw', 'ff_cisco_subscr_fbs_data_v_rpo') }}
),

final AS (
    SELECT
        subscription_ref_id,
        subscription_id,
        subscription_status,
        item_id,
        ccw_order_line_id,
        line_status,
        billing_model,
        billing_preference,
        order_type,
        offer_type,
        previous_ccw_order_line_id,
        item_action_detail,
        bill_date,
        charge_cycle_start,
        charge_cycle_end,
        currency_code,
        item_total,
        billing_source,
        last_modified_date
    FROM source_ff_cisco_subscr_fbs_data_v_rpo
)

SELECT * FROM final