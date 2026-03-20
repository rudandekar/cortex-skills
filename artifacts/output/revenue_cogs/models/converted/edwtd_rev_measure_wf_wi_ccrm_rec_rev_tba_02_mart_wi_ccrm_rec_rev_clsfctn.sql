{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccrm_rec_rev_tba', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCRM_REC_REV_TBA',
        'target_table': 'WI_CCRM_REC_REV_CLSFCTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.698790+00:00'
    }
) }}

WITH 

source_wi_ccrm_rr_defrl_recog_vert AS (
    SELECT
        fiscal_year_month_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_adjustment_type_cd,
        product_key,
        dv_product_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        service_flg,
        sales_order_key,
        deferral_amt,
        release_amt
    FROM {{ source('raw', 'wi_ccrm_rr_defrl_recog_vert') }}
),

source_wi_ccrm_rec_rev_clsfctn AS (
    SELECT
        fiscal_year_month_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_adjustment_type_cd,
        dd_bk_direct_corp_adj_type_cd,
        dv_comp_us_net_rev_amt,
        product_key,
        dv_product_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        service_flg,
        sales_order_key,
        revenue_classification,
        recurring_flag
    FROM {{ source('raw', 'wi_ccrm_rec_rev_clsfctn') }}
),

source_wi_gl_rec_rev_net_rel_amt AS (
    SELECT
        fiscal_year_month_int,
        deal_id,
        sales_territory_key,
        dv_sales_order_line_key,
        bk_adjustment_type_cd,
        dd_bk_direct_corp_adj_type_cd,
        net_rev,
        product_key,
        dv_product_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dv_service_flg,
        edw_so_key
    FROM {{ source('raw', 'wi_gl_rec_rev_net_rel_amt') }}
),

source_el_sm_ccrm_tba_rec_rev AS (
    SELECT
        tba_revenue_measure_key,
        fiscal_year_month_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_adjustment_type_cd,
        dd_bk_direct_corp_adj_type_cd,
        product_key,
        dv_product_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        service_flg,
        sales_order_key,
        revenue_classification,
        recurring_flag,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'el_sm_ccrm_tba_rec_rev') }}
),

source_wi_ccrm_rec_rev_defrl_recog AS (
    SELECT
        fiscal_year_month_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_adjustment_type_cd,
        dd_bk_direct_corp_adj_type_cd,
        dv_comp_us_net_rev_amt,
        product_key,
        dv_product_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        service_flg,
        sales_order_key
    FROM {{ source('raw', 'wi_ccrm_rec_rev_defrl_recog') }}
),

source_wi_ccrm_rec_rev_tba AS (
    SELECT
        tba_revenue_measure_key,
        fiscal_year_month_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_adjustment_type_cd,
        dd_bk_direct_corp_adj_type_cd,
        dv_comp_us_net_rev_amt,
        product_key,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        dv_product_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        service_flg,
        sales_order_key,
        revenue_classification,
        recurring_flag,
        action_code,
        dml_type,
        dv_recurring_offer_cd,
        dv_rev_class_rule_name
    FROM {{ source('raw', 'wi_ccrm_rec_rev_tba') }}
),

source_wi_ccrm_rec_rev_net_rel_amt AS (
    SELECT
        fiscal_year_month_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_adjustment_type_cd,
        product_key,
        dv_product_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        service_flg,
        sales_order_key,
        deferral_amt,
        release_amt,
        net_release_amt
    FROM {{ source('raw', 'wi_ccrm_rec_rev_net_rel_amt') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_adjustment_type_cd,
        dd_bk_direct_corp_adj_type_cd,
        dv_comp_us_net_rev_amt,
        product_key,
        dv_product_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        service_flg,
        sales_order_key,
        revenue_classification,
        recurring_flag
    FROM source_wi_ccrm_rec_rev_net_rel_amt
)

SELECT * FROM final