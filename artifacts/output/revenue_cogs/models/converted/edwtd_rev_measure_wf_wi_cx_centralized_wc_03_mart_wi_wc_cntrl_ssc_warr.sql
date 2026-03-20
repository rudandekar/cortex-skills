{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cx_centralized_wc', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CX_CENTRALIZED_WC',
        'target_table': 'WI_WC_CNTRL_SSC_WARR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.139228+00:00'
    }
) }}

WITH 

source_wi_wc_cntrl_ssc_warr_total_pct AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hw_family,
        ssc_total_cost,
        ssc_warranty_cost,
        ssc_pct
    FROM {{ source('raw', 'wi_wc_cntrl_ssc_warr_total_pct') }}
),

source_wi_wc_ts_cntrl_platforms_wc AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hybrid_product_family,
        ts_cntrl_platforms_wc_cost
    FROM {{ source('raw', 'wi_wc_ts_cntrl_platforms_wc') }}
),

source_wi_wc_cntrl_ssc_total AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hw_family,
        ssc_total_cost
    FROM {{ source('raw', 'wi_wc_cntrl_ssc_total') }}
),

source_wi_wc_cntrl_tac_by_pf AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hybrid_product_family,
        tac_total_cost
    FROM {{ source('raw', 'wi_wc_cntrl_tac_by_pf') }}
),

source_wi_wc_cntrl_tac_warr_detail AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hybrid_product_family,
        tac_total_cost,
        tac_warr_detail_cost
    FROM {{ source('raw', 'wi_wc_cntrl_tac_warr_detail') }}
),

source_wi_wc_cntrl_ssc_warr AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hw_family,
        ssc_warranty_cost
    FROM {{ source('raw', 'wi_wc_cntrl_ssc_warr') }}
),

source_wi_wc_cntrl_allctd_tac_pf_mtch AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hybrid_product_family,
        ssc_pct,
        tac_total_cost,
        tac_warr_detail_cost,
        tac_warr_cost,
        tac_allocated_cost
    FROM {{ source('raw', 'wi_wc_cntrl_allctd_tac_pf_mtch') }}
),

source_wi_wc_cntrl_allctd_tac_pct AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hybrid_product_family,
        tac_pct
    FROM {{ source('raw', 'wi_wc_cntrl_allctd_tac_pct') }}
),

final AS (
    SELECT
        fiscal_year_month,
        wc_trnx_fiscal_year_month_int,
        hw_family,
        ssc_warranty_cost
    FROM source_wi_wc_cntrl_allctd_tac_pct
)

SELECT * FROM final