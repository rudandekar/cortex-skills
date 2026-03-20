{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_material_trx_retro_update', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_MATERIAL_TRX_RETRO_UPDATE',
        'target_table': 'N_MATERIAL_TRANSACTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.828484+00:00'
    }
) }}

WITH 

source_w_material_transaction AS (
    SELECT
        material_transaction_key,
        cip_comment,
        ru_tan_inventory_org_key,
        ru_nr2a_product_inv_org_key,
        ru_nr2a_product_item_key,
        ru_tan_item_key,
        reason_cd,
        actual_cost_usd_amt,
        ru_product_inventory_org_key,
        ru_product_item_key,
        transaction_dtm,
        r2a_stock_type,
        ru_bk_geographic_location_cd,
        transaction_qty,
        adjusted_qty,
        sk_transaction_id_int,
        reference_type,
        inventory_organization_key,
        bk_transaction_type_name,
        ss_cd,
        lst_updtd_cisco_wrkr_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_purchase_order_key,
        ru_sales_order_key,
        ru_sales_order_line_key,
        bk_source_type_name,
        costed_flg,
        dv_transaction_dt,
        bk_subinventory_name,
        ru_transaction_reference_descr,
        receiving_transaction_key,
        source_create_dtm,
        dv_source_create_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_material_transaction') }}
),

final AS (
    SELECT
        material_transaction_key,
        cip_comment,
        ru_tan_inventory_org_key,
        ru_nr2a_product_inv_org_key,
        ru_nr2a_product_item_key,
        ru_tan_item_key,
        reason_cd,
        actual_cost_usd_amt,
        ru_product_inventory_org_key,
        ru_product_item_key,
        transaction_dtm,
        r2a_stock_type,
        ru_bk_geographic_location_cd,
        transaction_qty,
        adjusted_qty,
        sk_transaction_id_int,
        reference_type,
        inventory_organization_key,
        bk_transaction_type_name,
        ss_cd,
        lst_updtd_cisco_wrkr_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_purchase_order_key,
        ru_sales_order_key,
        ru_sales_order_line_key,
        bk_source_type_name,
        costed_flg,
        dv_transaction_dt,
        bk_subinventory_name,
        ru_transaction_reference_descr,
        receiving_transaction_key,
        source_create_dtm,
        dv_source_create_dt,
        ep_transaction_set_id_int,
        stock_locator_key
    FROM source_w_material_transaction
)

SELECT * FROM final