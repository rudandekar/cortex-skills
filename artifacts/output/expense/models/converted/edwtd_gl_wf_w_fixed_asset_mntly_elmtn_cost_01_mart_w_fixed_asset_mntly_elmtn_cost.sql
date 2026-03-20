{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_fixed_asset_mntly_elmtn_cost', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_FIXED_ASSET_MNTLY_ELMTN_COST',
        'target_table': 'W_FIXED_ASSET_MNTLY_ELMTN_COST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.763197+00:00'
    }
) }}

WITH 

source_w_fixed_asset_mntly_elmtn_cost AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_fixed_asset_num,
        bk_company_cd,
        set_of_books_key,
        bk_asset_event_life_cycle_cd,
        elmntn_dprctn_usd_amt,
        dv_elimination_usd_cost,
        cogs_usd_cost,
        depreciation_dt_usd_amt,
        depreciation_dt_functional_amt,
        functional_currency_cd,
        elmntn_dprctn_functional_amt,
        dv_elimination_functional_cost,
        dprctn_reserve_fin_acct_cd,
        bk_department_cd,
        dv_elimination_nbv_usd_amt,
        dv_elimination_nbv_functional_amt,
        remaining_usd_amt,
        remaining_functional_amt,
        general_ledger_account_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fixed_asset_mntly_elmtn_cost') }}
),

source_st_fa_mnthly_eln AS (
    SELECT
        elimination_id,
        asset_id,
        book_type,
        period_name,
        monthly_counter,
        asset_event,
        elim_amount,
        remaining_amount,
        depreciation_to_date,
        department,
        company,
        cat_segment1,
        cat_segment2,
        je_header_id,
        cogs_amount,
        asset_cost,
        asset_orig_cost,
        asset_cost_usd,
        asset_orig_cost_usd,
        asset_account,
        depr_account,
        deprn_amount,
        last_update_date,
        last_updated_by,
        created_by,
        creation_date,
        last_update_login,
        functional_currency,
        conversion_rate,
        deprn_reserve_acct
    FROM {{ source('raw', 'st_fa_mnthly_eln') }}
),

final AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_fixed_asset_num,
        bk_company_cd,
        set_of_books_key,
        bk_asset_event_life_cycle_cd,
        elmntn_dprctn_usd_amt,
        dv_elimination_usd_cost,
        cogs_usd_cost,
        depreciation_dt_usd_amt,
        depreciation_dt_functional_amt,
        functional_currency_cd,
        elmntn_dprctn_functional_amt,
        dv_elimination_functional_cost,
        dprctn_reserve_fin_acct_cd,
        bk_department_cd,
        dv_elimination_nbv_usd_amt,
        dv_elimination_nbv_functional_amt,
        remaining_usd_amt,
        remaining_functional_amt,
        general_ledger_account_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_fa_mnthly_eln
)

SELECT * FROM final