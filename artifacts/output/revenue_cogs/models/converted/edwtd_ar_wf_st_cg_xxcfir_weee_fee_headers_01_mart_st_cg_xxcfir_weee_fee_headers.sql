{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg_xxcfir_weee_fee_headers', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CG_XXCFIR_WEEE_FEE_HEADERS',
        'target_table': 'ST_CG_XXCFIR_WEEE_FEE_HEADERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.174816+00:00'
    }
) }}

WITH 

source_cg1_xxcfir_weee_fee_headers AS (
    SELECT
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
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_xxcfir_weee_fee_headers') }}
),

final AS (
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
    FROM source_cg1_xxcfir_weee_fee_headers
)

SELECT * FROM final