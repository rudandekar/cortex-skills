{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_gl_cogs_delta', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_GL_COGS_DELTA',
        'target_table': 'W_GL_COGS_DELTA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.269028+00:00'
    }
) }}

WITH 

source_wi_gl_rev_measure_cogs AS (
    SELECT
        sales_order_key,
        sales_order_line_key,
        ru_parnt_sls_order_line_key,
        bk_so_number_int,
        bk_so_line_number_int,
        sk_so_line_id_int,
        erp_parent_line_id,
        item_type_code,
        bk_product_id,
        ru_bk_product_family_id,
        bk_business_unit_id,
        bk_technology_group_id,
        sk_inventory_item_id_int,
        ss_code,
        dd_shipment_confirmed_dtm,
        bk_ship_from_inv_org_name_code,
        dv_extended_qty,
        dv_extended_net_qty,
        dv_comp_us_ext_selling_prc_amt,
        unit_sale_price,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_cost_amt,
        dv_fiscal_year_mth_number_int,
        dv_comp_us_edw_cost_amt
    FROM {{ source('raw', 'wi_gl_rev_measure_cogs') }}
),

transformed_ex_w_gl_cogs_delta AS (
    SELECT
    sales_order_line_key,
    sales_order_key,
    ru_parnt_sls_order_line_key,
    bk_so_number_int,
    bk_so_line_number_int,
    sk_so_line_id_int,
    erp_parent_line_id,
    item_type_code,
    bk_product_id,
    ru_bk_product_family_id,
    bk_business_unit_id,
    bk_technology_group_id,
    sk_inventory_item_id_int,
    bk_ship_from_inv_org_name_code,
    ss_code,
    dd_shipment_confirmed_dtm,
    dv_fiscal_year_mth_number_int,
    mtl_transaction_dtm,
    unit_sale_price_amt,
    dv_extended_qty,
    dv_comp_us_ext_selling_prc_amt,
    dv_comp_us_net_cost_amt,
    dv_comp_us_gross_cost_amt,
    total_mta_cost,
    total_edw_cost,
    dv_comp_us_cogs_delta_amt,
    edw_create_user,
    edw_create_datetime
    FROM source_wi_gl_rev_measure_cogs
),

final AS (
    SELECT
        sales_order_line_key,
        sales_order_key,
        ru_parnt_sls_order_line_key,
        bk_so_number_int,
        bk_so_line_number_int,
        sk_so_line_id_int,
        erp_parent_line_id,
        item_type_code,
        bk_product_id,
        ru_bk_product_family_id,
        bk_business_unit_id,
        bk_technology_group_id,
        sk_inventory_item_id_int,
        bk_ship_from_inv_org_name_code,
        ss_code,
        dd_shipment_confirmed_dtm,
        dv_fiscal_year_mth_number_int,
        mtl_transaction_dtm,
        unit_sale_price_amt,
        dv_extended_qty,
        dv_comp_us_ext_selling_prc_amt,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_mta_cost_amt,
        dv_comp_us_edw_cost_amt,
        dv_comp_us_cogs_delta_amt,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM transformed_ex_w_gl_cogs_delta
)

SELECT * FROM final