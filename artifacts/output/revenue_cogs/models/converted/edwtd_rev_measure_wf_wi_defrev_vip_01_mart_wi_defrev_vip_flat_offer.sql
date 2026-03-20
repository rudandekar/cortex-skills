{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_vip', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_VIP',
        'target_table': 'WI_DEFREV_VIP_FLAT_OFFER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.136007+00:00'
    }
) }}

WITH 

source_wi_defrev_vip AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_vip') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
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
        sk_offer_attribution_id_int,
        product_subscription_flg,
        dv_beginning_blnce_rev_usd_amt,
        deferral_rev_usd_amt
    FROM source_wi_defrev_vip
)

SELECT * FROM final