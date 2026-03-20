{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fixed_asset_trx_book', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIXED_ASSET_TRX_BOOK',
        'target_table': 'N_FIXED_ASSET_TRX_BOOK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.808014+00:00'
    }
) }}

WITH 

source_w_fixed_asset_trx_book AS (
    SELECT
        fixed_asset_transaction_key,
        remaining_life_months_cnt,
        current_cost_functional_amt,
        current_cost_usd_amt,
        net_depreciation_amt,
        fa_trx_book_ineffective_dtm,
        fa_trx_book_effective_dtm,
        original_cost_functional_amt,
        original_cost_usd_amt,
        ss_cd,
        sk_transaction_header_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        asset_depreciation_start_dt,
        asset_placed_in_service_dt,
        depreciation_method_cd,
        asst_depreciable_life_mths_cnt,
        asset_acquisition_dt,
        bk_fixed_asset_num,
        bk_company_cd,
        bk_set_of_books_key,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fixed_asset_trx_book') }}
),

final AS (
    SELECT
        fixed_asset_transaction_key,
        remaining_life_months_cnt,
        current_cost_functional_amt,
        current_cost_usd_amt,
        net_depreciation_amt,
        fa_trx_book_ineffective_dtm,
        fa_trx_book_effective_dtm,
        original_cost_functional_amt,
        original_cost_usd_amt,
        ss_cd,
        sk_transaction_header_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        asset_depreciation_start_dt,
        asset_placed_in_service_dt,
        depreciation_method_cd,
        asst_depreciable_life_mths_cnt,
        asset_acquisition_dt,
        bk_fixed_asset_num,
        bk_company_cd,
        bk_set_of_books_key
    FROM source_w_fixed_asset_trx_book
)

SELECT * FROM final