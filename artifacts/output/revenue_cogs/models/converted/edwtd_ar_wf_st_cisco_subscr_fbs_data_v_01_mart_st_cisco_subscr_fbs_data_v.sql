{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cisco_subscr_fbs_data_v', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CISCO_SUBSCR_FBS_DATA_V',
        'target_table': 'ST_CISCO_SUBSCR_FBS_DATA_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.431198+00:00'
    }
) }}

WITH 

source_ff_cisco_subscr_fbs_data_v AS (
    SELECT
        id,
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
        bill_date,
        charge_cycle_start,
        charge_cycle_end,
        currency_code,
        item_total,
        creation_date,
        last_modified_date,
        item_action,
        prev_order_line_id
    FROM {{ source('raw', 'ff_cisco_subscr_fbs_data_v') }}
),

final AS (
    SELECT
        id,
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
        bill_date,
        charge_cycle_start,
        charge_cycle_end,
        currency_code,
        item_total,
        creation_date,
        last_modified_date,
        item_action,
        prev_order_line_id
    FROM source_ff_cisco_subscr_fbs_data_v
)

SELECT * FROM final