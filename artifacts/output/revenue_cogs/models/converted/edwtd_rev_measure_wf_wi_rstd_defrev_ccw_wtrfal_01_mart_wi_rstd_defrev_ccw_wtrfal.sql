{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_defrev_ccw_wtrfal', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_DEFREV_CCW_WTRFAL',
        'target_table': 'WI_RSTD_DEFREV_CCW_WTRFAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.760311+00:00'
    }
) }}

WITH 

source_wi_rstd_defrev_ccw_wtrfal AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        product_key,
        orig_sales_territory_key,
        sales_territory_key,
        bk_deal_id,
        comp_us_net_rev_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        adjustment_trx_type_cd,
        pob_type_cd,
        restated_sls_crdt_split_pct
    FROM {{ source('raw', 'wi_rstd_defrev_ccw_wtrfal') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        product_key,
        orig_sales_territory_key,
        sales_territory_key,
        bk_deal_id,
        comp_us_net_rev_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        adjustment_trx_type_cd,
        pob_type_cd,
        restated_sls_crdt_split_pct
    FROM source_wi_rstd_defrev_ccw_wtrfal
)

SELECT * FROM final