{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_blt_bom_prdt_lnk_pst_asn', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_AS_BLT_BOM_PRDT_LNK_PST_ASN',
        'target_table': 'WI_AS_BLT_BOM_PRDT_KEY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.226614+00:00'
    }
) }}

WITH 

source_st_cg1_xxscm_mk_po_history AS (
    SELECT
        message_3b2_id,
        po_number,
        asn_process_date,
        message_bom_pack_id,
        bom_ref_id,
        bom_parent_ref_id,
        item_number,
        item_revision,
        item_type,
        uom,
        bom_item_quantity,
        org_code,
        flb_cost,
        df_fee_cost,
        currency_code,
        planning_code,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        cost_updated_flag,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_xxscm_mk_po_history') }}
),

final AS (
    SELECT
        ep_message_bom_pack_id,
        ep_bom_ref_id,
        ep_bom_parent_ref_id,
        src_rprtd_purchase_order_num,
        as_built_product_item_type_cd,
        item_number,
        item_key
    FROM source_st_cg1_xxscm_mk_po_history
)

SELECT * FROM final