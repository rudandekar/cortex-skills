{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcca_weee_fee_hdrs_cg', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCCA_WEEE_FEE_HDRS_CG',
        'target_table': 'EL_XXCCA_WEEE_FEE_HDRS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.597187+00:00'
    }
) }}

WITH 

source_st_xxcfir_weee_fee_headers AS (
    SELECT
        batch_id,
        weee_fee_header_id,
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
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        account_no,
        subaccount,
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
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcfir_weee_fee_headers') }}
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
    FROM source_st_xxcfir_weee_fee_headers
)

SELECT * FROM final