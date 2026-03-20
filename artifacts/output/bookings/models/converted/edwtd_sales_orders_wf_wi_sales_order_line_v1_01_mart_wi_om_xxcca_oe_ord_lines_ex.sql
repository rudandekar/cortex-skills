{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_order_line_v1', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_ORDER_LINE_V1',
        'target_table': 'WI_OM_XXCCA_OE_ORD_LINES_EX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.046655+00:00'
    }
) }}

WITH 

source_ex_om_oe_order_lines_all AS (
    SELECT
        line_id,
        line_number,
        global_name,
        shipment_number,
        option_number,
        ges_update_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        ato_line_id,
        shipping_method_code,
        split_from_line_id,
        exception_type,
        batch_id,
        create_datetime,
        action_code,
        tax_exempt_flag,
        link_to_line_id,
        top_model_line_id,
        shippable_flag,
        pricing_quantity
    FROM {{ source('raw', 'ex_om_oe_order_lines_all') }}
),

source_st_om_oe_order_lines_all AS (
    SELECT
        line_id,
        line_number,
        global_name,
        shipment_number,
        option_number,
        ges_update_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        ato_line_id,
        shipping_method_code,
        split_from_line_id,
        batch_id,
        create_datetime,
        tax_exempt_flag,
        action_code,
        link_to_line_id,
        top_model_line_id,
        shippable_flag,
        pricing_quantity
    FROM {{ source('raw', 'st_om_oe_order_lines_all') }}
),

source_ex_om_xxcca_oe_ord_lines_ex AS (
    SELECT
        line_id,
        global_name,
        attribute21,
        dispatch_date,
        dispatch_result_code,
        contract_quote_number,
        ges_update_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute12,
        receivable_intf_result_code,
        receivable_intf_date,
        batch_id,
        create_datetime,
        action_code,
        attribute20,
        exception_type,
        ship_to_po_number,
        delivery_option,
        attribute6,
        taa,
        attribute1,
        attribute2,
        attribute4,
        attribute5,
        clean_config_flag,
        attribute25,
        attribute23,
        attribute14,
        attribute7
    FROM {{ source('raw', 'ex_om_xxcca_oe_ord_lines_ex') }}
),

source_st_om_xxcca_oe_ord_lines_ex AS (
    SELECT
        line_id,
        global_name,
        attribute21,
        dispatch_date,
        dispatch_result_code,
        contract_quote_number,
        ges_update_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute12,
        receivable_intf_result_code,
        receivable_intf_date,
        batch_id,
        create_datetime,
        action_code,
        attribute20,
        attribute5,
        attribute1,
        attribute2,
        ship_to_po_number,
        delivery_option,
        attribute6,
        attribute4,
        taa,
        clean_config_flag,
        flexible_service_start_date,
        attribute25,
        attribute23,
        attribute14,
        attribute7
    FROM {{ source('raw', 'st_om_xxcca_oe_ord_lines_ex') }}
),

source_wi_st_om_xxcca_oe_ord_lines_ex AS (
    SELECT
        line_id,
        global_name,
        attribute21,
        dispatch_date,
        dispatch_result_code,
        contract_quote_number,
        ges_update_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        batch_id,
        create_datetime,
        action_code,
        sales_order_line_key,
        product_key,
        sk_inventory_item_id_int,
        bk_ship_from_inv_org_name_code,
        goods_prod_sls_order_line_key,
        accts_rcvbl_interface_dtm,
        accts_rcvbl_interface_stat_cd,
        bundle_goods_product_key,
        constituent_goods_product_key,
        taa,
        flexible_service_start_date
    FROM {{ source('raw', 'wi_st_om_xxcca_oe_ord_lines_ex') }}
),

source_wi_prntq_nsolt_catid AS (
    SELECT
        sales_order_line_key,
        line_id,
        global_name,
        product_key,
        bk_ship_from_inv_org_name_code,
        sk_organization_id_int,
        sk_inventory_item_id_int,
        category_id
    FROM {{ source('raw', 'wi_prntq_nsolt_catid') }}
),

source_wi_prntq_nsolt_orgid AS (
    SELECT
        sales_order_line_key,
        line_id,
        global_name,
        product_key,
        bk_ship_from_inv_org_name_code,
        sk_organization_id_int,
        sk_inventory_item_id_int
    FROM {{ source('raw', 'wi_prntq_nsolt_orgid') }}
),

source_wi_prntq_nsolt_mf_catid AS (
    SELECT
        sales_order_line_key,
        line_id,
        global_name,
        product_key,
        bk_ship_from_inv_org_name_code,
        sk_organization_id_int,
        sk_inventory_item_id_int,
        category_id,
        segment3,
        mf_category_id
    FROM {{ source('raw', 'wi_prntq_nsolt_mf_catid') }}
),

final AS (
    SELECT
        line_id,
        global_name,
        attribute21,
        dispatch_date,
        dispatch_result_code,
        contract_quote_number,
        ges_update_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        batch_id,
        create_datetime,
        action_code,
        sales_order_line_key,
        product_key,
        sk_inventory_item_id_int,
        bk_ship_from_inv_org_name_code,
        goods_prod_sls_order_line_key,
        accts_rcvbl_interface_dtm,
        accts_rcvbl_interface_stat_cd,
        bundle_goods_product_key,
        ship_to_po_number,
        delivery_option,
        attribute6,
        constituent_goods_product_key,
        taa,
        clean_config_flag,
        flexible_service_start_date,
        attribute25,
        attribute23,
        attribute14,
        attribute7
    FROM source_wi_prntq_nsolt_mf_catid
)

SELECT * FROM final