{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fixed_asset_mntly_elmtn_cost', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIXED_ASSET_MNTLY_ELMTN_COST',
        'target_table': 'N_FIXED_ASSET_MNTLY_ELMTN_COST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.991284+00:00'
    }
) }}

WITH 

source_n_fixed_asset_mntly_elmtn_cost AS (
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
        edw_update_user
    FROM {{ source('raw', 'n_fixed_asset_mntly_elmtn_cost') }}
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
        edw_update_user
    FROM source_n_fixed_asset_mntly_elmtn_cost
)

SELECT * FROM final