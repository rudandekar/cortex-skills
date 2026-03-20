{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ap_vendor_invoice_batch', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_AP_VENDOR_INVOICE_BATCH',
        'target_table': 'EX_AP_BATCHES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.639513+00:00'
    }
) }}

WITH 

source_st_mf_ap_batches_all AS (
    SELECT
        batch_id,
        actual_invoice_count,
        actual_invoice_total,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category,
        batch_code_combination_id,
        batch_date,
        batchid_source,
        batch_name,
        control_invoice_count,
        control_invoice_total,
        created_by,
        creation_date,
        doc_category_code,
        ges_update_date,
        global_name,
        gl_date,
        hold_lookup_code,
        hold_reason,
        invoice_currency_code,
        invoice_type_lookup_code,
        last_updated_by,
        last_update_date,
        last_update_login,
        org_id,
        payment_currency_code,
        payment_priority,
        pay_group_lookup_code,
        terms_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_ap_batches_all') }}
),

source_w_ap_vendor_invoice_batch AS (
    SELECT
        ap_vendor_invoice_batch_key,
        bk_apvi_batch_name,
        bk_operating_unit_name_cd,
        apvi_control_invoice_cnt,
        apvi_control_invoice_total_amt,
        apvi_batch_dtm,
        source_deleted_flg,
        sk_batch_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ap_vendor_invoice_batch') }}
),

transformed_exp_w_ap_vendor_invoice_batch AS (
    SELECT
    ap_vendor_invoice_batch_key,
    batch_name,
    bk_operating_unit_name_cd,
    control_invoice_count,
    control_invoice_total,
    batch_date,
    source_deleted_flg,
    batchid_source,
    global_name,
    action_code,
    dml_type
    FROM source_w_ap_vendor_invoice_batch
),

final AS (
    SELECT
        batch_id,
        batch_date,
        batchid_source,
        batch_name,
        control_invoice_count,
        control_invoice_total,
        global_name,
        org_id,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_w_ap_vendor_invoice_batch
)

SELECT * FROM final