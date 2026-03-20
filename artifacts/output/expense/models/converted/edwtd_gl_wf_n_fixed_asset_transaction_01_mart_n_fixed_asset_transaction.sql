{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fixed_asset_transaction', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIXED_ASSET_TRANSACTION',
        'target_table': 'N_FIXED_ASSET_TRANSACTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.907697+00:00'
    }
) }}

WITH 

source_w_fixed_asset_transaction AS (
    SELECT
        fixed_asset_transaction_key,
        bk_fixed_asset_num,
        bk_company_cd,
        bk_set_of_books_key,
        bk_fa_transaction_dtm,
        bk_fa_transaction_type_cd,
        ss_cd,
        sk_transaction_header_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        fa_transaction_effective_dtm,
        dd_fa_book_type_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fixed_asset_transaction') }}
),

final AS (
    SELECT
        fixed_asset_transaction_key,
        bk_fixed_asset_num,
        bk_company_cd,
        bk_set_of_books_key,
        bk_fa_transaction_dtm,
        bk_fa_transaction_type_cd,
        ss_cd,
        sk_transaction_header_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        fa_transaction_effective_dtm,
        dd_fa_book_type_cd
    FROM source_w_fixed_asset_transaction
)

SELECT * FROM final