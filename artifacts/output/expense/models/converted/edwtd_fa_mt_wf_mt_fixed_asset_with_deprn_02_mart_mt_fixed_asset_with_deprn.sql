{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_fixed_asset_with_deprn', 'batch', 'edwtd_fa_mt'],
    meta={
        'source_workflow': 'wf_m_MT_FIXED_ASSET_WITH_DEPRN',
        'target_table': 'MT_FIXED_ASSET_WITH_DEPRN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.967810+00:00'
    }
) }}

WITH 

source_w_fixed_asset_trx_detail AS (
    SELECT
        fixed_asset_trx_detail_key,
        set_of_books_key,
        asset_qty,
        fa_transaction_detail_type,
        ss_cd,
        sk_book_type_cd,
        sk_distribution_id_int,
        sk_period_counter_int,
        sk_ges_pk_id_int,
        general_ledger_account_key,
        journal_entry_num_int,
        company_cd,
        journal_entry_line_num_int,
        fixed_asset_transaction_key,
        bk_fa_transaction_dtl_type_cd,
        ru_fa_transaction_debit_amt,
        ru_to_physical_location_name,
        ru_fa_transaction_credit_amt,
        ru_from_physical_location_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dd_fixed_asset_num,
        bk_journal_entry_batch_id_int,
        ss_table_cd,
        fa_trx_depreciation_role,
        fa_trx_detail_effective_dt,
        ru_bk_fa_book_type_cd,
        ru_bk_dprctn_prd_seq_num_int,
        action_code,
        dml_type,
        dv_ban_id
    FROM {{ source('raw', 'w_fixed_asset_trx_detail') }}
),

final AS (
    SELECT
        fiscal_year_month_num_int,
        bk_company_cd,
        bk_department_cd,
        fixed_asset_num,
        dv_asset_account_cd,
        cip_account_cd,
        expense_account_cd,
        serial_num,
        fa_tag_num,
        model_num,
        asset_descr,
        asset_acquisition_dt,
        asset_depreciation_start_dt,
        asset_placed_in_service_dt,
        internal_equipment_role,
        asst_depreciabl_life_mth_cnt,
        dv_life_yr_mnt,
        original_cost_functional_amt,
        original_cost_usd_amt,
        books_current_cost,
        books_usd_cost,
        bk_fa_book_type_cd,
        remaining_life_months_cnt,
        ru_internal_eqpmnt_asset_qty,
        functional_currency_cd,
        to_physical_location_name,
        from_physical_location_name,
        current_cost_functional_amt,
        dv_current_cost_usd_amt,
        monthly_deprn_amt,
        dv_usd_monthly_deprn_amt,
        yr_to_dt_deprn_func_amt,
        dv_yr_to_dt_deprn_usd_amt,
        deprn_reserve_functional_amt,
        usd_deprn_reserve_usd_amt,
        dv_functional_net_book_val_amt,
        dv_net_book_val_usd_amt,
        dv_depreciation_type,
        dv_ban_id
    FROM source_w_fixed_asset_trx_detail
)

SELECT * FROM final