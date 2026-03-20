{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_attr_prdt_new_rnwl_acv_swoa', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_ATTR_PRDT_NEW_RNWL_ACV_SWOA',
        'target_table': 'WI_SWOA_ERP_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.096949+00:00'
    }
) }}

WITH 

source_wi_swoa_rr_rule_tag AS (
    SELECT
        curr_sol_key,
        sk_attribution_id_int,
        renewal_ref_cd,
        renewal_ref_id,
        renewal_sol_key,
        renewal_ref_value,
        renewal_ref_type,
        reason_code,
        process_flag
    FROM {{ source('raw', 'wi_swoa_rr_rule_tag') }}
),

source_wi_swoa_bkgs_xaas_prev_hist AS (
    SELECT
        dv_so_sbscrptn_itm_sls_trx_key,
        offer_attribution_id_int,
        attributed_inv_item_id,
        attributed_product,
        coverage_sd,
        coverage_ed,
        end_customer,
        cav_bu_id,
        attr_prdt_class_name,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        ordered_inv_item_id,
        order_product_cd,
        service_flg,
        valid_coverage,
        cisco_booked_dtm,
        item_catalog_group_cd,
        customer_party_key,
        bk_product_subgroup_id,
        dd_comp_us_list_price_amount,
        dd_comp_us_net_price_amount
    FROM {{ source('raw', 'wi_swoa_bkgs_xaas_prev_hist') }}
),

source_wi_swoa_bkgs_erp_prev_hist AS (
    SELECT
        order_number,
        order_line_id,
        offer_attribution_id_int,
        attributed_inv_item_id,
        attributed_product,
        coverage_sd,
        coverage_ed,
        end_customer,
        cav_bu_id,
        attr_prdt_class_name,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        ordered_inv_item_id,
        order_product_cd,
        service_flg,
        valid_coverage,
        cisco_booked_dtm,
        item_catalog_group_cd,
        customer_party_key,
        bk_product_subgroup_id,
        dd_comp_us_list_price_amount,
        dd_comp_us_net_price_amount
    FROM {{ source('raw', 'wi_swoa_bkgs_erp_prev_hist') }}
),

source_wi_swoa_erp_incr AS (
    SELECT
        sales_order_line_key,
        sk_attribution_id_int,
        offer_attrib_prdt_key,
        attr_prdt_class_name,
        process_flg,
        renewal_ref_id,
        renewal_ref_cd,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        cx_customer_bu_id,
        bk_product_subgroup_id
    FROM {{ source('raw', 'wi_swoa_erp_incr') }}
),

source_wi_swoa_erp_rules_tags AS (
    SELECT
        sales_order_line_key,
        sk_attribution_id_int,
        ru_service_contract_start_dtm,
        process_flag,
        renewal_ref_cd,
        renewal_ref_id,
        renewal_gap_days,
        reason_cd
    FROM {{ source('raw', 'wi_swoa_erp_rules_tags') }}
),

final AS (
    SELECT
        sales_order_line_key,
        sk_attribution_id_int,
        offer_attrib_prdt_key,
        attr_prdt_class_name,
        process_flg,
        renewal_ref_id,
        renewal_ref_cd,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        cx_customer_bu_id,
        bk_product_subgroup_id
    FROM source_wi_swoa_erp_rules_tags
)

SELECT * FROM final