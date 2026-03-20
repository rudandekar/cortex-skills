{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_ccw_srvc_net_trngltd', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_CCW_SRVC_NET_TRNGLTD',
        'target_table': 'WI_SRVC_NETSYS_TRNGT_IN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.414242+00:00'
    }
) }}

WITH 

source_wi_srvc_netsys_trngt_out AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        trngtd_sales_territory_key,
        be_trngtd_allocation_pct,
        driver_type
    FROM {{ source('raw', 'wi_srvc_netsys_trngt_out') }}
),

source_wi_srvc_netsys_threshold_type AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        l3_sales_territory_name_code,
        amt,
        percentage,
        thresold_type
    FROM {{ source('raw', 'wi_srvc_netsys_threshold_type') }}
),

source_wi_srvc_netsys_trngt_in AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        l3_sales_territory_name_code,
        iso_country_code,
        product_key,
        bk_product_id,
        ru_bk_product_family_id,
        bk_business_entity_name,
        prdt_family_allocation_pct
    FROM {{ source('raw', 'wi_srvc_netsys_trngt_in') }}
),

source_wi_defrev_ccw_srvc_net_flat AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        dv_beginning_blnce_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_ccw_srvc_net_flat') }}
),

source_wi_defrev_ccw_srvc_net AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        rev_measurement_type_cd,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_ccw_srvc_net') }}
),

source_wi_defrev_ccw_srvc_net_offer AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        dv_beginning_blnce_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_ccw_srvc_net_offer') }}
),

source_wi_defrev_ccw_srvc_net_trngltd AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        erp_deal_id,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        dv_beginning_blnce_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_ccw_srvc_net_trngltd') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        fiscal_year_month_int,
        sales_territory_key,
        l3_sales_territory_name_code,
        iso_country_code,
        product_key,
        bk_product_id,
        ru_bk_product_family_id,
        bk_business_entity_name,
        prdt_family_allocation_pct
    FROM source_wi_defrev_ccw_srvc_net_trngltd
)

SELECT * FROM final