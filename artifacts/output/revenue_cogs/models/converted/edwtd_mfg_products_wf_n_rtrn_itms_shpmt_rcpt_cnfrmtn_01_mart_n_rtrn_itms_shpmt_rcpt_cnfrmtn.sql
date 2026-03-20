{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rtrn_itms_shpmt_rcpt_cnfrmtn', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RTRN_ITMS_SHPMT_RCPT_CNFRMTN',
        'target_table': 'N_RTRN_ITMS_SHPMT_RCPT_CNFRMTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.013460+00:00'
    }
) }}

WITH 

source_w_rtrn_itms_shpmt_rcpt_cnfrmtn AS (
    SELECT
        bk_carton_id,
        bk_purchase_order_num,
        create_dt,
        process_status_cd,
        transmission_dt,
        rtrn_itms_rcpt_cnfrmtn_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rtrn_itms_shpmt_rcpt_cnfrmtn') }}
),

final AS (
    SELECT
        bk_carton_id,
        bk_purchase_order_num,
        create_dt,
        process_status_cd,
        transmission_dt,
        rtrn_itms_rcpt_cnfrmtn_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_rtrn_itms_shpmt_rcpt_cnfrmtn
)

SELECT * FROM final