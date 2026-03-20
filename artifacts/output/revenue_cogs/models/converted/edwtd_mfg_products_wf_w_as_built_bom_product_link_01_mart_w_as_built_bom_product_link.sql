{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_built_bom_product_link', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_AS_BUILT_BOM_PRODUCT_LINK',
        'target_table': 'W_AS_BUILT_BOM_PRODUCT_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.214282+00:00'
    }
) }}

WITH 

source_st_cg1_xxscm_mk_as_bilt_bom AS (
    SELECT
        bom_ref_id,
        bom_parent_ref_id,
        po_number,
        so_number,
        so_line_number,
        so_major_line_number,
        cisco_duns,
        partner_duns,
        bom_item_quantity,
        uom,
        item_type,
        item_number,
        org_code,
        item_revision,
        item_price,
        currency_code,
        planning_code,
        transmission_date,
        process_status,
        bom_process_flag,
        bom_process_status,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        request_id,
        flb_cost,
        message_bom_pack_id,
        ges_update_date,
        global_name,
        df_fee_cost,
        batch_id,
        create_datetime,
        action_code,
        attribute6
    FROM {{ source('raw', 'st_cg1_xxscm_mk_as_bilt_bom') }}
),

final AS (
    SELECT
        as_built_bom_product_link_key,
        sales_order_line_key,
        inventory_organization_key,
        product_key,
        constituent_part_key,
        carton_id,
        ep_message_bom_pack_id,
        ep_bom_ref_id,
        ep_bom_parent_ref_id,
        planning_cd,
        bom_infrmtn_transmission_dtm,
        create_dtm,
        cnstnt_part_unit_cost_usd_amt,
        constituent_part_qty,
        src_rprtd_purchase_order_num,
        purchase_order_key,
        as_built_product_item_type_cd,
        as_blt_cnstnt_part_item_rev_cd,
        bom_process_status_cd,
        process_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        df_fee_usd_cost,
        dv_rollup_usd_cost,
        last_updated_dtm,
        dv_last_updated_dt,
        cost_rollup_dtm,
        dv_cost_rollup_dt,
        action_code,
        dml_type
    FROM source_st_cg1_xxscm_mk_as_bilt_bom
)

SELECT * FROM final