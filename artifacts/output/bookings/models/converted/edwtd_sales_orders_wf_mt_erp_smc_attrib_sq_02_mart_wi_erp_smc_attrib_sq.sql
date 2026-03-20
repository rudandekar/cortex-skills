{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_erp_smc_attrib_sq', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_ERP_SMC_ATTRIB_SQ',
        'target_table': 'WI_ERP_SMC_ATTRIB_SQ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.557128+00:00'
    }
) }}

WITH 

source_wi_mt_summary_quote_sol_alloc AS (
    SELECT
        sales_order_line_key,
        dv_allocation_pct,
        dv_effective_dtm,
        dv_expiration_dtm,
        sales_motion_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sk_sq_sol_alloc_key,
        sk_offer_attribution_id_int,
        attributed_product,
        as_ts_code,
        as_architecture_name,
        technology_group_id,
        attr_prdt_offer_type_name,
        reason_code,
        manual_override_flag,
        comments,
        case_number,
        user_id
    FROM {{ source('raw', 'wi_mt_summary_quote_sol_alloc') }}
),

source_wi_erp_smc_attrib_sq AS (
    SELECT
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
        offer_attrib_prdt_key
    FROM {{ source('raw', 'wi_erp_smc_attrib_sq') }}
),

source_mt_sls_motion_attribution AS (
    SELECT
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
    FROM {{ source('raw', 'mt_sls_motion_attribution') }}
),

final AS (
    SELECT
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
        offer_attrib_prdt_key
    FROM source_mt_sls_motion_attribution
)

SELECT * FROM final