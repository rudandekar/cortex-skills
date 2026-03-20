{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcca_weee_fee_hdrs', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCCA_WEEE_FEE_HDRS',
        'target_table': 'EL_XXCCA_WEEE_FEE_HDRS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.026217+00:00'
    }
) }}

WITH 

source_st_om_xxcca_weee_fee_hdrs AS (
    SELECT
        batch_id,
        weee_fee_header_id,
        global_name,
        org_id,
        country,
        weee_fee_inv_item_id,
        weee_fee_item_name,
        weee_fee_currency,
        sold_to_org_id,
        state_or_province,
        weee_fee_description,
        start_date_active,
        end_date_active,
        created_by,
        last_updated_by,
        last_update_login,
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
        create_datetime,
        action_code,
        ges_update_date,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'st_om_xxcca_weee_fee_hdrs') }}
),

final AS (
    SELECT
        weee_fee_header_id,
        global_name,
        org_id,
        country,
        weee_fee_inv_item_id,
        weee_fee_item_name,
        weee_fee_currency,
        sold_to_org_id,
        state_or_province,
        weee_fee_description,
        start_date_active,
        end_date_active,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10
    FROM source_st_om_xxcca_weee_fee_hdrs
)

SELECT * FROM final