{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_subscr_line_billing_schedule', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_SUBSCR_LINE_BILLING_SCHEDULE',
        'target_table': 'EX_CISCO_SUBSCR_FBS_DATA_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.645187+00:00'
    }
) }}

WITH 

source_w_subscr_line_billing_schedule AS (
    SELECT
        subscr_line_billing_schedule_key,
        bk_iso_currency_code,
        bk_product_key,
        bk_dv_billing_schedule_int,
        bk_subscr_reference_id,
        bk_exaas_subscr_num,
        subscr_status_code,
        subscr_line_status_code,
        billing_model_name,
        billing_preference_code,
        sales_order_type_code,
        billing_preference_model_start_dt,
        billing_preference_model_end_dt,
        item_total_sale_val_local_amt,
        source_created_dtm,
        dv_source_created_dt,
        source_last_updated_dtm,
        dv_source_last_updated_dt,
        offer_type_code,
        subscr_invoice_release_dtm,
        dv_subscr_invoice_release_dt,
        sales_order_line_key,
        action_cd,
        prev_ref_sales_order_line_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_subscr_line_billing_schedule') }}
),

source_sm_subscription_line_fbs AS (
    SELECT
        subscr_line_billing_schedule_key,
        subscription_ref_id,
        bk_exaas_subscr_num,
        product_key,
        bill_date,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_subscription_line_fbs') }}
),

source_ex_cisco_subscr_fbs_data_v AS (
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
        exception_type,
        item_action,
        prev_order_line_id
    FROM {{ source('raw', 'ex_cisco_subscr_fbs_data_v') }}
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
        exception_type,
        item_action,
        prev_order_line_id
    FROM source_ex_cisco_subscr_fbs_data_v
)

SELECT * FROM final