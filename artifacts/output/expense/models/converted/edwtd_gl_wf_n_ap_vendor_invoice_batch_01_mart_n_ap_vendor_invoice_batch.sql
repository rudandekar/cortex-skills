{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ap_vendor_invoice_batch', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_AP_VENDOR_INVOICE_BATCH',
        'target_table': 'N_AP_VENDOR_INVOICE_BATCH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.141212+00:00'
    }
) }}

WITH 

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

final AS (
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
        edw_update_dtm
    FROM source_w_ap_vendor_invoice_batch
)

SELECT * FROM final