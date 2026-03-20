{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_vendor_payment_term_line', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_VENDOR_PAYMENT_TERM_LINE',
        'target_table': 'N_VENDOR_PAYMENT_TERM_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.887459+00:00'
    }
) }}

WITH 

source_w_vendor_payment_term_line AS (
    SELECT
        vendor_payment_term_key,
        bk_term_line_num_int,
        payment_due_pct_name,
        payment_due_trxl_amt,
        payment_due_days_rmng_cnt,
        payment_due_day_of_mth_int,
        discount_pct,
        discount_days_cnt,
        discount_day_of_mth_int,
        second_discount_pct,
        second_discount_days_cnt,
        second_discount_day_of_mth_int,
        third_discount_pct,
        third_discount_days_cnt,
        third_discount_day_of_mth_int,
        special_calendar_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_vendor_payment_term_line') }}
),

final AS (
    SELECT
        vendor_payment_term_key,
        bk_term_line_num_int,
        payment_due_pct_name,
        payment_due_trxl_amt,
        payment_due_days_rmng_cnt,
        payment_due_day_of_mth_int,
        discount_pct,
        discount_days_cnt,
        discount_day_of_mth_int,
        second_discount_pct,
        second_discount_days_cnt,
        second_discount_day_of_mth_int,
        third_discount_pct,
        third_discount_days_cnt,
        third_discount_day_of_mth_int,
        special_calendar_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_vendor_payment_term_line
)

SELECT * FROM final