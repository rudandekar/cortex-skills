{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_return_products_to_bts', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_RETURN_PRODUCTS_TO_BTS',
        'target_table': 'EX_CG1_X_MK_3C8R2AHDRS_IFAC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.788690+00:00'
    }
) }}

WITH 

source_st_cg1_x_mk_3c8r2ahdrs_ifac AS (
    SELECT
        message_id,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        r2a_ref_no,
        transmission_date,
        partner_name,
        org_code,
        cisco_duns_number,
        partner_duns_number,
        return_from,
        b2b_mfg_status,
        carrier,
        tracking_no,
        retry_count,
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
        expected_arrival_date,
        ges_update_date,
        global_name,
        batch_id,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_cg1_x_mk_3c8r2ahdrs_ifac') }}
),

final AS (
    SELECT
        message_id,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        r2a_ref_no,
        transmission_date,
        partner_name,
        org_code,
        cisco_duns_number,
        partner_duns_number,
        return_from,
        b2b_mfg_status,
        carrier,
        tracking_no,
        retry_count,
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
        expected_arrival_date,
        ges_update_date,
        global_name,
        batch_id,
        action_code,
        create_datetime,
        exception_type
    FROM source_st_cg1_x_mk_3c8r2ahdrs_ifac
)

SELECT * FROM final