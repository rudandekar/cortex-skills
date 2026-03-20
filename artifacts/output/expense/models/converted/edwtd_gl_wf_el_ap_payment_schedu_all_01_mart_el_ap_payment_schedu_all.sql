{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ap_payment_schedu_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_AP_PAYMENT_SCHEDU_ALL',
        'target_table': 'EL_AP_PAYMENT_SCHEDU_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.931322+00:00'
    }
) }}

WITH 

source_st_mf_ap_payment_schedu_all AS (
    SELECT
        batch_id,
        amount_remaining,
        attribute_category,
        attribute1,
        attribute15,
        invoice_batch_id,
        checkrun_id,
        created_by,
        creation_date,
        discount_amount_available,
        discount_amount_remaining,
        discount_date,
        due_date,
        external_bank_account_id,
        future_pay_due_date,
        global_name,
        ges_update_date,
        gross_amount,
        hold_flag,
        inv_curr_gross_amount,
        invoice_id,
        last_update_date,
        last_update_login,
        last_updated_by,
        org_id,
        payment_cross_rate,
        payment_method_lookup_code,
        payment_num,
        payment_priority,
        payment_status_flag,
        second_disc_amt_available,
        second_discount_date,
        third_disc_amt_available,
        third_discount_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_ap_payment_schedu_all') }}
),

final AS (
    SELECT
        global_name,
        ges_update_date,
        hold_flag,
        invoice_id,
        payment_num,
        org_id,
        creation_date
    FROM source_st_mf_ap_payment_schedu_all
)

SELECT * FROM final