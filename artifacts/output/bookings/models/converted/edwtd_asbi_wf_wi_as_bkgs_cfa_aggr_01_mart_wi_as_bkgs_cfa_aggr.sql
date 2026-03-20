{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_bkgs_cfa_aggr', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_BKGS_CFA_AGGR',
        'target_table': 'WI_AS_BKGS_CFA_AGGR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.481959+00:00'
    }
) }}

WITH 

source_wi_as_bkgs_cfa_aggr AS (
    SELECT
        fiscal_year_week_num_int,
        fiscal_year_mth_number_int,
        sales_territory_key,
        product_key,
        bill_to_customer_key,
        end_customer_key,
        sold_to_customer_key,
        ship_to_customer_key,
        adjustment_code,
        dv_sales_order_line_key,
        bkgs_measure_trans_type_cd,
        service_flg,
        corporate_bookings_flg,
        revenue_recognition_flg,
        attribution_cd,
        dv_deal_id,
        bk_service_category_id,
        dv_goods_adj_prd_key,
        bk_allocated_servc_group_id,
        recurring_offer_cd,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        sales_order_key,
        dv_true_up_flg,
        dv_gsp_or_cx_product,
        ide_adjustment_code,
        cancel_code,
        dv_transaction_type,
        xcat_flg,
        bk_offer_type_name,
        recurring_offer_flg,
        ela_flg,
        dd_comp_us_net_price_amt,
        comp_us_cost_amount,
        annualized_us_net_amt,
        multiyear_us_net_amt,
        annuity_amt,
        edw_update_dtm
    FROM {{ source('raw', 'wi_as_bkgs_cfa_aggr') }}
),

final AS (
    SELECT
        fiscal_year_week_num_int,
        fiscal_year_mth_number_int,
        sales_territory_key,
        product_key,
        bill_to_customer_key,
        end_customer_key,
        sold_to_customer_key,
        ship_to_customer_key,
        adjustment_code,
        dv_sales_order_line_key,
        bkgs_measure_trans_type_cd,
        service_flg,
        corporate_bookings_flg,
        revenue_recognition_flg,
        attribution_cd,
        dv_deal_id,
        bk_service_category_id,
        dv_goods_adj_prd_key,
        bk_allocated_servc_group_id,
        recurring_offer_cd,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        sales_order_key,
        dv_true_up_flg,
        dv_gsp_or_cx_product,
        ide_adjustment_code,
        cancel_code,
        dv_transaction_type,
        xcat_flg,
        bk_offer_type_name,
        recurring_offer_flg,
        ela_flg,
        dd_comp_us_net_price_amt,
        comp_us_cost_amount,
        annualized_us_net_amt,
        multiyear_us_net_amt,
        annuity_amt,
        edw_update_dtm
    FROM source_wi_as_bkgs_cfa_aggr
)

SELECT * FROM final