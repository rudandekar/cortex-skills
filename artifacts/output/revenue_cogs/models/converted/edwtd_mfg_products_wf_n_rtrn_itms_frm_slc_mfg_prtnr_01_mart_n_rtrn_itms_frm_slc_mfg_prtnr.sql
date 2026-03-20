{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rtrn_itms_frm_slc_mfg_prtnr', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RTRN_ITMS_FRM_SLC_MFG_PRTNR',
        'target_table': 'N_RTRN_ITMS_FRM_SLC_MFG_PRTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.546212+00:00'
    }
) }}

WITH 

source_w_rtrn_itms_frm_slc_mfg_prtnr AS (
    SELECT
        rtrn_itms_frmslc_mfg_prtnr_key,
        process_status_cd,
        return_from_cd,
        create_dt,
        transmission_dt,
        sales_order_key,
        inventory_organization_key,
        sk_message_id,
        purchase_order_key,
        purchase_order_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rtrn_itms_frm_slc_mfg_prtnr') }}
),

final AS (
    SELECT
        rtrn_itms_frmslc_mfg_prtnr_key,
        process_status_cd,
        return_from_cd,
        create_dt,
        transmission_dt,
        sales_order_key,
        inventory_organization_key,
        sk_message_id,
        purchase_order_key,
        purchase_order_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_rtrn_itms_frm_slc_mfg_prtnr
)

SELECT * FROM final