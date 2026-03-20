{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_xaas_smc_attrib_nrtr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_XAAS_SMC_ATTRIB_NRTR',
        'target_table': 'MT_XAAS_SLS_MOTION_ATTRIB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.755829+00:00'
    }
) }}

WITH 

source_wi_mt_xaas_smc_attrib_nrtr AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        dv_oa_flg,
        start_tv_dtm,
        end_tv_dtm,
        dv_source_type,
        as_architecture_name,
        technology_group_id,
        attr_prdt_offer_type_name,
        as_ts_code,
        sk_renew_contract_line_id,
        sk_as_parent_inventory_item_id,
        sk_offer_attribution_id_int,
        product_class,
        transaction_cr_party_key,
        hq_cr_prty_key,
        renewal_ref_id,
        renewal_ref_cd,
        smr_tagging_failure_rsn_cd,
        sales_motion_timing_cd,
        offer_attrib_prdt_key
    FROM {{ source('raw', 'wi_mt_xaas_smc_attrib_nrtr') }}
),

source_mt_xaas_sls_motion_attrib AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        dv_oa_flg,
        start_tv_dtm,
        end_tv_dtm,
        dv_source_type,
        as_architecture_name,
        technology_group_id,
        attr_prdt_offer_type_name,
        as_ts_cd,
        sk_renew_contract_line_id,
        sk_as_parent_inventory_item_id,
        sk_offer_attribution_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        src_enterprise_inv_sku_id,
        product_class,
        transaction_cr_party_key,
        hq_cr_prty_key,
        renewal_ref_id,
        renewal_ref_cd,
        smr_tagging_failure_rsn_cd,
        sales_motion_timing_cd,
        manual_override_role,
        requesting_csco_wrkr_prty_key,
        sls_mtn_correction_case_num,
        sls_mtn_correction_cmnt,
        offer_attrib_prdt_key,
        renewal_gap_days,
        bundle_flg,
        renewal_ref_rule
    FROM {{ source('raw', 'mt_xaas_sls_motion_attrib') }}
),

final AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        dv_oa_flg,
        start_tv_dtm,
        end_tv_dtm,
        dv_source_type,
        as_architecture_name,
        technology_group_id,
        attr_prdt_offer_type_name,
        as_ts_cd,
        sk_renew_contract_line_id,
        sk_as_parent_inventory_item_id,
        sk_offer_attribution_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        src_enterprise_inv_sku_id,
        product_class,
        transaction_cr_party_key,
        hq_cr_prty_key,
        renewal_ref_id,
        renewal_ref_cd,
        smr_tagging_failure_rsn_cd,
        sales_motion_timing_cd,
        manual_override_role,
        requesting_csco_wrkr_prty_key,
        sls_mtn_correction_case_num,
        sls_mtn_correction_cmnt,
        offer_attrib_prdt_key,
        renewal_gap_days,
        bundle_flg,
        renewal_ref_rule
    FROM source_mt_xaas_sls_motion_attrib
)

SELECT * FROM final