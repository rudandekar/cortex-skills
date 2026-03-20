{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_periodic_billing_schedule', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_PERIODIC_BILLING_SCHEDULE',
        'target_table': 'N_PERIODIC_BILLING_SCHEDULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.411235+00:00'
    }
) }}

WITH 

source_n_periodic_billing_schedule AS (
    SELECT
        sales_order_line_key,
        bk_ss_cd,
        bk_billing_schedule_num_int,
        ar_trx_key,
        bk_iso_currency_cd,
        billing_schedule_batch_id,
        billing_status_name,
        bill_local_amt,
        invoiced_product_qty,
        invoice_local_amt,
        billing_term_start_dtm,
        dv_billing_term_start_dt,
        billing_term_end_dtm,
        dv_billing_term_end_dt,
        bill_rel_to_invoicing_dtm,
        dv_bill_rel_to_invoicing_dt,
        invoice_dtm,
        dv_invoice_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_periodic_billing_schedule') }}
),

final AS (
    SELECT
        sales_order_line_key,
        bk_ss_cd,
        bk_billing_schedule_num_int,
        ar_trx_key,
        bk_iso_currency_cd,
        billing_schedule_batch_id,
        billing_status_name,
        bill_local_amt,
        invoiced_product_qty,
        invoice_local_amt,
        billing_term_start_dtm,
        dv_billing_term_start_dt,
        billing_term_end_dtm,
        dv_billing_term_end_dt,
        bill_rel_to_invoicing_dtm,
        dv_bill_rel_to_invoicing_dt,
        invoice_dtm,
        dv_invoice_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_periodic_billing_schedule
)

SELECT * FROM final