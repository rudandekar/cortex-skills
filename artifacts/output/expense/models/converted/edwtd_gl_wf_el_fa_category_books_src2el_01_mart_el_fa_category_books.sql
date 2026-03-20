{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fa_category_books_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_FA_CATEGORY_BOOKS_SRC2EL',
        'target_table': 'EL_FA_CATEGORY_BOOKS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.815167+00:00'
    }
) }}

WITH 

source_mf_fa_category_books AS (
    SELECT
        asset_clearing_account_ccid,
        asset_clearing_acct,
        asset_cost_account_ccid,
        asset_cost_acct,
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
        bonus_deprn_expense_acct,
        bonus_deprn_reserve_acct,
        bonus_reserve_acct_ccid,
        book_type_code,
        category_id,
        cip_clearing_acct,
        cip_cost_acct,
        created_by,
        creation_date,
        default_group_asset_id,
        deprn_expense_acct,
        deprn_reserve_acct,
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
        last_updated_by,
        last_update_date,
        last_update_login,
        life_extension_ceiling,
        life_extension_factor,
        percent_salvage_value,
        reserve_account_ccid,
        reval_amortization_acct,
        reval_amort_account_ccid,
        reval_reserve_account_ccid,
        reval_reserve_acct,
        wip_clearing_account_ccid,
        wip_cost_account_ccid
    FROM {{ source('raw', 'mf_fa_category_books') }}
),

lookup_lkptrans AS (
    SELECT
        a.*,
        b.*
    FROM source_mf_fa_category_books a
    LEFT JOIN {{ source('raw', 'el_fa_category_books') }} b
        ON a.in_book_type_code = b.in_book_type_code
),

transformed_exptrans AS (
    SELECT
    asset_cost_account_ccid,
    book_type_code,
    category_id,
    global_name,
    wip_cost_account_ccid,
    TO_INTEGER(ASSET_COST_ACCOUNT_CCID) AS out_asset_cost_account_ccid,
    TO_INTEGER(CATEGORY_ID) AS out_category_id
    FROM lookup_lkptrans
),

routed_rtrtrans AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exptrans
),

transformed_exp_el_fa_category_books AS (
    SELECT
    asset_cost_account_ccid,
    book_type_code,
    category_id,
    global_name,
    wip_cost_account_ccid,
    lkp_asset_cost_account_ccid,
    lkp_book_type_code,
    lkp_category_id,
    lkp_global_name,
    lkp_wip_cost_account_ccid
    FROM routed_rtrtrans
),

update_strategy_updtrns_insert AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_el_fa_category_books
    WHERE DD_INSERT != 3
),

update_strategy_updtrns_update AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_updtrns_insert
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        asset_cost_account_ccid,
        book_type_code,
        category_id,
        global_name,
        wip_cost_account_ccid
    FROM update_strategy_updtrns_update
)

SELECT * FROM final