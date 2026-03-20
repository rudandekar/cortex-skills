{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_fixed_asset_transaction_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FIXED_ASSET_TRANSACTION_TYPE',
        'target_table': 'W_FIXED_ASSET_TRANSACTION_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.119855+00:00'
    }
) }}

WITH 

source_st_mf_fa_txn_headers AS (
    SELECT
        batch_id,
        amortization_start_date,
        asset_id,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category_code,
        book_type_code,
        calling_interface,
        date_effective,
        ges_update_date,
        global_name,
        invoice_transaction_id,
        last_updated_by,
        last_update_date,
        last_update_login,
        mass_reference_id,
        mass_transaction_id,
        source_transaction_header_id,
        transaction_date_entered,
        transaction_header_id,
        transaction_key,
        transaction_name,
        transaction_subtype,
        transaction_type_code,
        event_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fa_txn_headers') }}
),

final AS (
    SELECT
        bk_fa_transaction_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_mf_fa_txn_headers
)

SELECT * FROM final