{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_subscr_line_billing_schedule', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_SUBSCR_LINE_BILLING_SCHEDULE',
        'target_table': 'EL_SUBSCRIPTION_LINE_ATTR_BS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.355796+00:00'
    }
) }}

WITH 

source_el_subscription_line_attr_bs AS (
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
        attributed_product_key,
        attribution_cd,
        attribution_pct,
        prev_ref_sales_order_line_key
    FROM {{ source('raw', 'el_subscription_line_attr_bs') }}
),

source_n_subscr_line_billing_schedule AS (
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
        edw_update_user
    FROM {{ source('raw', 'n_subscr_line_billing_schedule') }}
),

final AS (
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
        attributed_product_key,
        attribution_cd,
        attribution_pct,
        prev_ref_sales_order_line_key
    FROM source_n_subscr_line_billing_schedule
)

SELECT * FROM final