{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bookings_hier_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_BOOKINGS_HIER_AS',
        'target_table': 'WI_BOOKINGS_HIER_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.379312+00:00'
    }
) }}

WITH 

source_wi_bookings_hier_as AS (
    SELECT
        fiscal_year_week_num_int,
        fiscal_year_mth_number_int,
        sales_territory_key,
        product_key,
        sales_order_line_key,
        bill_to_customer_key,
        end_customer_key,
        sold_to_customer_key,
        ship_to_customer_key,
        ide_adjustment_code,
        iso_country_code,
        dv_product_type,
        dv_pnl_book_net_cost_type,
        dv_bookings_line_item_name,
        bookings_amount,
        dv_bookings_or_cogs_type_cd,
        deal_ss_cd,
        dv_deal_id,
        xcat_flg,
        bk_offer_type_name,
        recurring_offer_flg,
        ela_flg,
        bkgs_measure_trans_type_code,
        bkgs_fiscal_year_mth_num_int,
        fcm_flag,
        goods_product_key,
        dv_tss_alctn_mthd_type_id_int,
        bk_allocated_servc_group_id,
        ru_bk_product_subgroup_id,
        dv_recurring_offer_cd,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_gsp_or_cx_product
    FROM {{ source('raw', 'wi_bookings_hier_as') }}
),

final AS (
    SELECT
        fiscal_year_week_num_int,
        fiscal_year_mth_number_int,
        sales_territory_key,
        product_key,
        sales_order_line_key,
        bill_to_customer_key,
        end_customer_key,
        sold_to_customer_key,
        ship_to_customer_key,
        ide_adjustment_code,
        iso_country_code,
        dv_product_type,
        dv_pnl_book_net_cost_type,
        dv_bookings_line_item_name,
        bookings_amount,
        dv_bookings_or_cogs_type_cd,
        deal_ss_cd,
        dv_deal_id,
        xcat_flg,
        bk_offer_type_name,
        recurring_offer_flg,
        ela_flg,
        bkgs_measure_trans_type_code,
        bkgs_fiscal_year_mth_num_int,
        fcm_flag,
        goods_product_key,
        dv_tss_alctn_mthd_type_id_int,
        bk_allocated_servc_group_id,
        ru_bk_product_subgroup_id,
        dv_recurring_offer_cd,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_gsp_or_cx_product
    FROM source_wi_bookings_hier_as
)

SELECT * FROM final