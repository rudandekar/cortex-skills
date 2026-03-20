{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sw_el_pos_trans_incr', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_SW_EL_POS_TRANS_INCR',
        'target_table': 'WI_SW_EL_POS_TRANS_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.592910+00:00'
    }
) }}

WITH 

source_wi_sw_el_pos_trans_incr AS (
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
        item_categorization_cd,
        try_and_buy_flg,
        cisco_booked_dtm,
        sales_order_line_key,
        process_flag,
        dv_contract_number,
        item_catalog_group_cd,
        bk_ship_to_wips_site_use_key,
        customer_party_key,
        subscription_product_key,
        license_product_key
    FROM {{ source('raw', 'wi_sw_el_pos_trans_incr') }}
),

final AS (
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
        item_categorization_cd,
        try_and_buy_flg,
        cisco_booked_dtm,
        sales_order_line_key,
        process_flag,
        dv_contract_number,
        item_catalog_group_cd,
        bk_ship_to_wips_site_use_key,
        customer_party_key,
        subscription_product_key,
        license_product_key
    FROM source_wi_sw_el_pos_trans_incr
)

SELECT * FROM final