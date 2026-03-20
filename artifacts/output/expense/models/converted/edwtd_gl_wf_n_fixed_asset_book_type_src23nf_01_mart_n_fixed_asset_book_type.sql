{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fixed_asset_book_type_src23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIXED_ASSET_BOOK_TYPE_SRC23NF',
        'target_table': 'N_FIXED_ASSET_BOOK_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.847281+00:00'
    }
) }}

WITH 

source_mf_fa_book_controls AS (
    SELECT
        accounting_flex_structure,
        allow_cip_assets_flag,
        allow_cost_ceiling,
        allow_deprn_adjustments,
        allow_deprn_exp_ceiling,
        allow_mass_changes,
        allow_mass_copy,
        allow_purge_flag,
        allow_reval_flag,
        amortize_flag,
        amortize_reval_reserve_flag,
        ap_intercompany_acct,
        ar_intercompany_acct,
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
        book_class,
        book_type_code,
        book_type_name,
        calculate_nbv,
        capital_gain_threshold,
        copy_additions_flag,
        copy_adjustments_flag,
        copy_retirements_flag,
        copy_salvage_value_flag,
        cost_of_removal_clearing_acct,
        cost_of_removal_gain_acct,
        cost_of_removal_loss_acct,
        created_by,
        creation_date,
        current_fiscal_year,
        date_ineffective,
        default_life_extension_ceiling,
        default_life_extension_factor,
        default_max_fully_rsvd_revals,
        default_reval_fully_rsvd_flag,
        deferred_deprn_expense_acct,
        deferred_deprn_reserve_acct,
        deprn_adjustment_acct,
        deprn_allocation_code,
        deprn_calendar,
        deprn_request_id,
        deprn_status,
        depr_first_year_ret_flag,
        distribution_source_book,
        fiscal_year_name,
        flexbuilder_defaults_ccid,
        fully_reserved_flag,
        ges_update_date,
        global_attribute1,
        global_attribute10,
        global_attribute11,
        global_attribute12,
        global_attribute13,
        global_attribute14,
        global_attribute15,
        global_attribute16,
        global_attribute17,
        global_attribute18,
        global_attribute19,
        global_attribute2,
        global_attribute20,
        global_attribute3,
        global_attribute4,
        global_attribute5,
        global_attribute6,
        global_attribute7,
        global_attribute8,
        global_attribute9,
        global_attribute_category,
        global_name,
        gl_je_source,
        gl_posting_allowed_flag,
        immediate_copy_flag,
        initial_date,
        initial_period_counter,
        itc_allowed_flag,
        je_addition_category,
        je_adjustment_category,
        je_cip_addition_category,
        je_cip_adjustment_category,
        je_cip_reclass_category,
        je_cip_retirement_category,
        je_cip_reval_category,
        je_cip_transfer_category,
        je_deferred_deprn_category,
        je_depreciation_category,
        je_deprn_adjustment_category,
        je_reclass_category,
        je_retirement_category,
        je_reval_category,
        je_transfer_category,
        last_deprn_run_date,
        last_mass_copy_period_counter,
        last_period_counter,
        last_purge_period_counter,
        last_updated_by,
        last_update_date,
        last_update_login,
        mass_copy_source_book,
        mass_request_id,
        mc_source_flag,
        nbv_amount_threshold,
        nbv_fraction_threshold,
        nbv_retired_gain_acct,
        nbv_retired_loss_acct,
        org_id,
        proceeds_of_sale_clearing_acct,
        proceeds_of_sale_gain_acct,
        proceeds_of_sale_loss_acct,
        prorate_calendar,
        retire_reval_reserve_flag,
        revalue_on_retirement_flag,
        reval_deprn_reserve_flag,
        reval_posting_flag,
        reval_rsv_retired_gain_acct,
        reval_rsv_retired_loss_acct,
        reval_ytd_deprn_flag,
        run_year_end_program,
        set_of_books_id,
        use_current_nbv_for_deprn,
        use_percent_salvage_value_flag
    FROM {{ source('raw', 'mf_fa_book_controls') }}
),

lookup_lkp_n_fixed_asset_book_type AS (
    SELECT
        a.*,
        b.*
    FROM source_mf_fa_book_controls a
    LEFT JOIN {{ source('raw', 'n_fixed_asset_book_type') }} b
        ON a.in_book_type_code = b.in_book_type_code
),

transformed_exp_n_fixed_asset_book_type AS (
    SELECT
    lkp_bk_fa_book_type_cd,
    bk_fa_book_type_cd,
    fa_book_class_code,
    fa_book_name,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM lookup_lkp_n_fixed_asset_book_type
),

routed_rtr_n_fixed_asset_book_type AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exp_n_fixed_asset_book_type
),

update_strategy_ins_upd_n_fixed_asset_book_type AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtr_n_fixed_asset_book_type
    WHERE DD_INSERT != 3
),

update_strategy_upd_upd_n_fixed_asset_book_type AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_ins_upd_n_fixed_asset_book_type
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        bk_fa_book_type_cd,
        fa_book_class_cd,
        fa_book_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_upd_n_fixed_asset_book_type
)

SELECT * FROM final