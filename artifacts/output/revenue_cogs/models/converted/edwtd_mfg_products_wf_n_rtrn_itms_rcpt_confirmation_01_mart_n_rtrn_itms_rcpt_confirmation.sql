{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rtrn_itms_rcpt_confirmation', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RTRN_ITMS_RCPT_CONFIRMATION',
        'target_table': 'N_RTRN_ITMS_RCPT_CONFIRMATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.689638+00:00'
    }
) }}

WITH 

source_w_rtrn_itms_rcpt_confirmation AS (
    SELECT
        rtrn_itms_rcpt_cnfrmtn_key,
        receipt_dtm,
        create_dt,
        process_status_cd,
        transmission_dt,
        sk_message_id,
        purchase_order_num,
        purchase_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rtrn_itms_rcpt_confirmation') }}
),

final AS (
    SELECT
        rtrn_itms_rcpt_cnfrmtn_key,
        receipt_dtm,
        create_dt,
        process_status_cd,
        transmission_dt,
        sk_message_id,
        purchase_order_num,
        purchase_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_rtrn_itms_rcpt_confirmation
)

SELECT * FROM final