{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fa_mnthly_eln', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_FA_MNTHLY_ELN',
        'target_table': 'ST_FA_MNTHLY_ELN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.969937+00:00'
    }
) }}

WITH 

source_ff_fa_mnthly_eln AS (
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
    FROM {{ source('raw', 'ff_fa_mnthly_eln') }}
),

final AS (
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
    FROM source_ff_fa_mnthly_eln
)

SELECT * FROM final