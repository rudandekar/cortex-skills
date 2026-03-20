{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_blt_bom_prdt_lnk_pst_asn', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_AS_BLT_BOM_PRDT_LNK_PST_ASN',
        'target_table': 'N_AS_BLT_BOM_PRDT_LNK_PST_ASN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.386254+00:00'
    }
) }}

WITH 

source_w_as_blt_bom_prdt_lnk_pst_asn AS (
    SELECT
        as_built_bom_product_link_key,
        asn_process_dtm,
        dv_asn_process_dt,
        inventory_orgn_name_key,
        product_key,
        constituent_part_key,
        ep_message_bom_pack_id,
        ep_bom_ref_id,
        ep_bom_parent_ref_id,
        planning_cd,
        create_dtm,
        last_updated_dtm,
        dv_last_updated_dt,
        cnstnt_part_unit_cost_usd_amt,
        df_fee_usd_cost,
        constituent_part_qty,
        src_rprtd_purchase_order_num,
        purchase_order_key,
        as_built_product_item_type_cd,
        as_blt_cnstnt_part_item_rev_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        brazil_specific_role,
        ru_bs_cofins_tax_usd_amt,
        ru_bs_icms_nrcvrbl_tax_usd_amt,
        ru_bs_rsrch_dvlpmt_tax_usd_amt,
        ru_bs_pis_tax_usd_amt,
        cost_rollup_dtm,
        dv_cost_rollup_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_as_blt_bom_prdt_lnk_pst_asn') }}
),

final AS (
    SELECT
        as_built_bom_product_link_key,
        asn_process_dtm,
        dv_asn_process_dt,
        inventory_orgn_name_key,
        product_key,
        constituent_part_key,
        ep_message_bom_pack_id,
        ep_bom_ref_id,
        ep_bom_parent_ref_id,
        planning_cd,
        create_dtm,
        last_updated_dtm,
        dv_last_updated_dt,
        cnstnt_part_unit_cost_usd_amt,
        df_fee_usd_cost,
        constituent_part_qty,
        src_rprtd_purchase_order_num,
        purchase_order_key,
        as_built_product_item_type_cd,
        as_blt_cnstnt_part_item_rev_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        brazil_specific_role,
        ru_bs_cofins_tax_usd_amt,
        ru_bs_icms_nrcvrbl_tax_usd_amt,
        ru_bs_rsrch_dvlpmt_tax_usd_amt,
        ru_bs_pis_tax_usd_amt,
        cost_rollup_dtm,
        dv_cost_rollup_dt
    FROM source_w_as_blt_bom_prdt_lnk_pst_asn
)

SELECT * FROM final