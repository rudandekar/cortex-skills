{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sw_bkgs_erp_pos_hist', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_SW_BKGS_ERP_POS_HIST',
        'target_table': 'WI_SW_BKGS_ERP_TRANS_HIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.850787+00:00'
    }
) }}

WITH 

source_wi_sw_bkgs_xaas_trans_hist AS (
    SELECT
        order_number,
        order_line_id,
        offer_attribution_id_int,
        attributed_inv_item_id,
        attributed_product,
        coverage_sd,
        coverage_ed,
        end_customer,
        end_customer_hq,
        attr_prdt_class_name,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        ordered_inv_item_id,
        order_product_cd,
        service_flg,
        valid_coverage,
        try_and_buy_flg,
        cisco_booked_dtm,
        dv_contract_number,
        item_catalog_group_cd,
        bk_ship_to_wips_site_use_key,
        customer_party_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        subscription_product_key,
        license_product_key,
        dd_comp_us_net_price_amount
    FROM {{ source('raw', 'wi_sw_bkgs_xaas_trans_hist') }}
),

source_wi_sw_bkgs_erp_trans_hist AS (
    SELECT
        order_number,
        order_line_id,
        offer_attribution_id_int,
        attributed_inv_item_id,
        attributed_product,
        coverage_sd,
        coverage_ed,
        end_customer,
        end_customer_hq,
        attr_prdt_class_name,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        ordered_inv_item_id,
        order_product_cd,
        service_flg,
        valid_coverage,
        try_and_buy_flg,
        cisco_booked_dtm,
        dv_contract_number,
        item_catalog_group_cd,
        bk_ship_to_wips_site_use_key,
        customer_party_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        subscription_product_key,
        license_product_key,
        dd_comp_us_net_price_amount
    FROM {{ source('raw', 'wi_sw_bkgs_erp_trans_hist') }}
),

source_wi_sw_bkgs_pos_trans_hist AS (
    SELECT
        pos_transaction_id_int,
        offer_attribution_id_int,
        attributed_inv_item_id,
        attributed_product,
        coverage_sd,
        coverage_ed,
        end_customer,
        end_customer_hq,
        attr_prdt_class_name,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        ordered_inv_item_id,
        order_product_cd,
        service_flg,
        valid_coverage,
        try_and_buy_flg,
        cisco_booked_dtm,
        dv_contract_number,
        item_catalog_group_cd,
        bk_ship_to_wips_site_use_key,
        customer_party_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        subscription_product_key,
        license_product_key,
        dd_comp_us_net_price_amount
    FROM {{ source('raw', 'wi_sw_bkgs_pos_trans_hist') }}
),

final AS (
    SELECT
        order_number,
        order_line_id,
        offer_attribution_id_int,
        attributed_inv_item_id,
        attributed_product,
        coverage_sd,
        coverage_ed,
        end_customer,
        end_customer_hq,
        attr_prdt_class_name,
        bkgs_measure_trans_type_code,
        dv_attribution_cd,
        ordered_inv_item_id,
        order_product_cd,
        service_flg,
        valid_coverage,
        try_and_buy_flg,
        cisco_booked_dtm,
        dv_contract_number,
        item_catalog_group_cd,
        bk_ship_to_wips_site_use_key,
        customer_party_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        subscription_product_key,
        license_product_key,
        dd_comp_us_net_price_amount
    FROM source_wi_sw_bkgs_pos_trans_hist
)

SELECT * FROM final