{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_service_as_fin_dfr_bal_by_pid', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SERVICE_AS_FIN_DFR_BAL_BY_PID',
        'target_table': 'WI_AS_FIN_DFR_UA_BE_PRD_ALLOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.459776+00:00'
    }
) }}

WITH 

source_wi_as_fin_dfr_bal_sol_rev AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        dfrev_as_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        rev_measurement_type_cd,
        transaction_seq_id,
        dv_recurring_offer_cd,
        rev_alloc_pct,
        total_balanced_rev_usd_amt
    FROM {{ source('raw', 'wi_as_fin_dfr_bal_sol_rev') }}
),

source_wi_service_as_fin_dfr_bal_by_pid AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        dfrev_as_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        rev_measurement_type_cd,
        transaction_seq_id,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        rev_alloc_pct,
        bkgs_alloc_pct,
        record_type,
        update_flag,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_service_as_fin_dfr_bal_by_pid') }}
),

source_wi_as_fin_dfr_sol_bkgs_pap AS (
    SELECT
        product_key,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        dd_comp_us_net_price_amt,
        bkgs_alloc_pct
    FROM {{ source('raw', 'wi_as_fin_dfr_sol_bkgs_pap') }}
),

source_wi_as_dfr_quote_sku_bkgs_atstb_pnl AS (
    SELECT
        product_key,
        bk_as_technology_name,
        bk_as_subtechnology_name,
        bk_as_architecture_name,
        bk_as_business_service_name,
        history_data_cd,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'wi_as_dfr_quote_sku_bkgs_atstb_pnl') }}
),

source_wi_as_fin_dfr_service_revenue_pp AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        dfrev_as_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        rev_measurement_type_cd,
        transaction_seq_id,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_as_fin_dfr_service_revenue_pp') }}
),

source_wi_as_fin_dfr_ua_be_prd_alloc AS (
    SELECT
        business_entity_descr,
        product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        be_split_pct,
        pc_split_pct,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product
    FROM {{ source('raw', 'wi_as_fin_dfr_ua_be_prd_alloc') }}
),

source_wi_as_fin_dfr_sol_bkgs AS (
    SELECT
        sales_order_key,
        sales_order_line_key,
        sales_territory_key,
        dv_attribution_cd,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        dd_comp_us_net_price_amt,
        bkgs_alloc_pct
    FROM {{ source('raw', 'wi_as_fin_dfr_sol_bkgs') }}
),

source_wi_be_pf_adj_pid_map_rev_as_fdfr AS (
    SELECT
        business_entity_descr,
        bk_product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        bk_product_id,
        item_key
    FROM {{ source('raw', 'wi_be_pf_adj_pid_map_rev_as_fdfr') }}
),

final AS (
    SELECT
        business_entity_descr,
        product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        be_split_pct,
        pc_split_pct,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product
    FROM source_wi_be_pf_adj_pid_map_rev_as_fdfr
)

SELECT * FROM final