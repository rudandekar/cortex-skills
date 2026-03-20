{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rtrn_itms_frm_slc_mfg_prtnr', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_RTRN_ITMS_FRM_SLC_MFG_PRTNR',
        'target_table': 'SM_RTRN_ITMS_FRM_SLC_MFG_PRTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.655564+00:00'
    }
) }}

WITH 

source_st_cg1_x_mk_3c8_hdr_ob_ifac AS (
    SELECT
        message_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        po_number,
        so_number,
        partner_name,
        org_code,
        cisco_duns_number,
        partner_duns_number,
        return_from,
        b2b_mfg_status,
        retry_count,
        transmission_date,
        process_status,
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
        request_id,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_x_mk_3c8_hdr_ob_ifac') }}
),

source_st_cg1_x_mk_3c8_hdr_ob_ifac AS (
    SELECT
        message_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        po_number,
        so_number,
        partner_name,
        org_code,
        cisco_duns_number,
        partner_duns_number,
        return_from,
        b2b_mfg_status,
        retry_count,
        transmission_date,
        process_status,
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
        request_id,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_x_mk_3c8_hdr_ob_ifac') }}
),

final AS (
    SELECT
        rtrn_itms_frmslc_mfg_prtnr_key,
        sk_message_id,
        edw_create_user,
        edw_create_dtm
    FROM source_st_cg1_x_mk_3c8_hdr_ob_ifac
)

SELECT * FROM final