{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_built_bom_product_link', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_AS_BUILT_BOM_PRODUCT_LINK',
        'target_table': 'EX_AS_BUILT_BOM_PRODUCT_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.392268+00:00'
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
        exception_type,
        attribute6
    FROM source_st_cg1_xxscm_mk_as_bilt_bom
)

SELECT * FROM final