{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_manual_jv_srvc', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_MANUAL_JV_SRVC',
        'target_table': 'WI_DEFREV_MANUAL_JV_SRVC_RANK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.566120+00:00'
    }
) }}

WITH 

source_wi_defrev_man_adj_recognized AS (
    SELECT
        processed_fiscal_year_mth_int,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        bk_deal_id,
        service_flg,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key
    FROM {{ source('raw', 'wi_defrev_man_adj_recognized') }}
),

source_wi_defrev_manual_jv_srvc_rank AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        rank_num,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_manual_jv_srvc_rank') }}
),

source_wi_defrev_manual_jv_srvc_flat AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        dv_beginning_blnce_rev_usd_amt,
        dv_bridge_balance_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_manual_jv_srvc_flat') }}
),

source_wi_defrev_manual_jv_srvc AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_manual_jv_srvc') }}
),

source_wi_defrev_mj_srvc_proj_max_mth AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_mj_srvc_proj_max_mth') }}
),

source_wi_defrev_mj_proj_discont_mth AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_mj_proj_discont_mth') }}
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
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        rank_num,
        sk_offer_attribution_id_int
    FROM source_wi_defrev_mj_proj_discont_mth
)

SELECT * FROM final