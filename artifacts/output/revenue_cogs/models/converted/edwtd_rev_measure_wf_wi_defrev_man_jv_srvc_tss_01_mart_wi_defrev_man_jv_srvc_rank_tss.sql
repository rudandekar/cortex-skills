{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_defrev_man_jv_srvc_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DEFREV_MAN_JV_SRVC_TSS',
        'target_table': 'WI_DEFREV_MAN_JV_SRVC_RANK_TSS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.392414+00:00'
    }
) }}

WITH 

source_wi_defrev_man_jv_srvc_ft_tss AS (
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
        sk_offer_attribution_id_int,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_defrev_man_jv_srvc_ft_tss') }}
),

source_wi_defrev_mj_discont_mth_tss AS (
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
        sk_offer_attribution_id_int,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_defrev_mj_discont_mth_tss') }}
),

source_wi_defrev_man_jv_srvc_rank_tss AS (
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
        sk_offer_attribution_id_int,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_defrev_man_jv_srvc_rank_tss') }}
),

source_wi_defrev_man_jv_srvc_tss AS (
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
        sk_offer_attribution_id_int,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_defrev_man_jv_srvc_tss') }}
),

source_wi_defrev_mj_srvc_max_mth_tss AS (
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
        sk_offer_attribution_id_int,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_defrev_mj_srvc_max_mth_tss') }}
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
        sk_offer_attribution_id_int,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM source_wi_defrev_mj_srvc_max_mth_tss
)

SELECT * FROM final