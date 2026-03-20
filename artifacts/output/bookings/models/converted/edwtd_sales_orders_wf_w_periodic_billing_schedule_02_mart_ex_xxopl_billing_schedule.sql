{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_periodic_billing_schedule', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_PERIODIC_BILLING_SCHEDULE',
        'target_table': 'EX_XXOPL_BILLING_SCHEDULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.088915+00:00'
    }
) }}

WITH 

source_st_xxgco_oe_invoice_schedules AS (
    SELECT
        so_header_id,
        so_line_id,
        bill_schedule_number,
        bill_schedule_id,
        bill_period_start_date,
        bill_period_end_date,
        bill_status_code,
        currency_code,
        bill_amount,
        bill_quantity,
        bill_release_date,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        program_id,
        last_login_id,
        invoice_number,
        invoice_date
    FROM {{ source('raw', 'st_xxgco_oe_invoice_schedules') }}
),

source_w_periodic_billing_schedule AS (
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
    FROM {{ source('raw', 'w_periodic_billing_schedule') }}
),

source_st_si_xxopl_billing_schedule AS (
    SELECT
        bill_schedule_number,
        bill_schedule_id,
        bill_period_end_date,
        bill_period_start_date,
        bill_status,
        currency_code,
        bill_amount,
        bill_release_date,
        bill_quantity,
        invoice_amount,
        invoice_date,
        invoice_number,
        header_id,
        line_id,
        line_ref_number,
        order_origin,
        record_offset,
        edw_create_dtm
    FROM {{ source('raw', 'st_si_xxopl_billing_schedule') }}
),

final AS (
    SELECT
        bill_schedule_number,
        bill_schedule_id,
        bill_period_end_date,
        bill_period_start_date,
        bill_status,
        currency_code,
        bill_amount,
        bill_release_date,
        bill_quantity,
        invoice_amount,
        invoice_date,
        invoice_number,
        header_id,
        line_id,
        line_ref_number,
        order_origin,
        record_offset,
        edw_create_dtm
    FROM source_st_si_xxopl_billing_schedule
)

SELECT * FROM final