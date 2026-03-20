{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_table_name_param_pnl_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_TABLE_NAME_PARAM_PNL_AS',
        'target_table': 'WI_BKGS_HIER_SRVCE_TRUEUP_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.740516+00:00'
    }
) }}

WITH 

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
),

source_wi_bkgs_hier_srvce_trueup_as AS (
    SELECT
        fiscal_year_week_num_int,
        fiscal_year_mth_number_int,
        sales_territory_key,
        product_key,
        bill_to_customer_key,
        end_customer_key,
        sold_to_customer_key,
        ship_to_customer_key,
        ide_adjustment_code,
        iso_country_code,
        product_type,
        sales_order_line_key,
        bkgs_measure_trans_type_code,
        dv_srvc_bookings_type,
        account_name,
        trueup_amt,
        deal_ss_cd,
        dv_deal_id,
        xcat_flg,
        bk_offer_type_name,
        recurring_offer_flg,
        ela_flg,
        goods_product_key,
        dv_tss_alctn_mthd_type_id_int,
        bk_allocated_servc_group_id,
        ru_bk_product_subgroup_id,
        dv_recurring_offer_cd,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_gsp_or_cx_product
    FROM {{ source('raw', 'wi_bkgs_hier_srvce_trueup_as') }}
),

transformed_param_expression AS (
    SELECT
    table_name1,
    table_name2,
    setvariable('MAP_VAR1',TABLE_NAME1) AS table_value1,
    setvariable('MAP_VAR2',TABLE_NAME2) AS table_value2
    FROM source_wi_bkgs_hier_srvce_trueup_as
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
        ide_adjustment_code,
        iso_country_code,
        product_type,
        sales_order_line_key,
        bkgs_measure_trans_type_code,
        dv_srvc_bookings_type,
        account_name,
        trueup_amt,
        deal_ss_cd,
        dv_deal_id,
        xcat_flg,
        bk_offer_type_name,
        recurring_offer_flg,
        ela_flg,
        goods_product_key,
        dv_tss_alctn_mthd_type_id_int,
        bk_allocated_servc_group_id,
        ru_bk_product_subgroup_id,
        dv_recurring_offer_cd,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_gsp_or_cx_product
    FROM transformed_param_expression
)

SELECT * FROM final