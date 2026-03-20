{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg_xxicm_saf_headers_rev', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CG_XXICM_SAF_HEADERS_REV',
        'target_table': 'ST_CG_XXICM_SAF_HEADERS_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.317231+00:00'
    }
) }}

WITH 

source_cg1_xxicm_saf_headers_all AS (
    SELECT
        saf_header_id,
        saf_id,
        saf_type,
        saf_reason,
        saf_date,
        saf_status,
        saf_amount,
        special_request,
        email_address,
        initiator_role_id,
        initiator_group_id,
        collector_id,
        comments,
        item_key,
        cash_receipt_id,
        alert_status,
        org_id,
        func_saf_amount,
        func_currency_code,
        tran_currency_code,
        exchange_rate_type,
        exchange_rate,
        exchange_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag,
        d_attribute1,
        n_attribute1,
        d_attribute2
    FROM {{ source('raw', 'cg1_xxicm_saf_headers_all') }}
),

final AS (
    SELECT
        batch_id,
        saf_header_id,
        saf_id,
        saf_type,
        saf_reason,
        saf_date,
        saf_status,
        saf_amount,
        special_request,
        email_address,
        initiator_role_id,
        initiator_group_id,
        collector_id,
        comments,
        item_key,
        cash_receipt_id,
        alert_status,
        org_id,
        func_saf_amount,
        func_currency_code,
        tran_currency_code,
        exchange_rate_type,
        exchange_rate,
        exchange_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_commit_time,
        global_name,
        create_datetime,
        action_code,
        d_attribute1,
        n_attribute1,
        d_attribute2,
        c_attribute8
    FROM source_cg1_xxicm_saf_headers_all
)

SELECT * FROM final