{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_vendor_payment_term_line', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_VENDOR_PAYMENT_TERM_LINE',
        'target_table': 'EX_CFN_AP_TERMS_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.780752+00:00'
    }
) }}

WITH 

source_st_cfn_ap_terms_lines AS (
    SELECT
        term_id,
        sequence_num,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        due_percent,
        due_amount,
        due_days,
        due_day_of_month,
        due_months_forward,
        discount_percent,
        discount_days,
        discount_day_of_month,
        discount_months_forward,
        discount_percent_2,
        discount_days_2,
        discount_day_of_month_2,
        discount_months_forward_2,
        discount_percent_3,
        discount_days_3,
        discount_day_of_month_3,
        discount_months_forward_3,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        fixed_date,
        calendar,
        discount_amount,
        discount_criteria,
        discount_amount_2,
        discount_criteria_2,
        discount_amount_3,
        discount_criteria_3
    FROM {{ source('raw', 'st_cfn_ap_terms_lines') }}
),

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
        term_id,
        sequence_num,
        due_percent,
        due_amount,
        due_days,
        due_day_of_month,
        discount_percent,
        discount_days,
        discount_day_of_month,
        discount_percent_2,
        discount_days_2,
        discount_day_of_month_2,
        discount_percent_3,
        discount_days_3,
        discount_day_of_month_3,
        calendar
    FROM source_w_vendor_payment_term_line
)

SELECT * FROM final