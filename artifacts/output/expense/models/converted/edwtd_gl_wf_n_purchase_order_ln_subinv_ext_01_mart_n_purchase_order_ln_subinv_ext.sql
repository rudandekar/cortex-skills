{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_purchase_order_ln_subinv_ext', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PURCHASE_ORDER_LN_SUBINV_EXT',
        'target_table': 'N_PURCHASE_ORDER_LN_SUBINV_EXT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.077298+00:00'
    }
) }}

WITH 

source_w_purchase_order_ln_subinv_ext AS (
    SELECT
        purchase_order_line_key,
        edcs_num,
        product_key,
        program_type_cd,
        minimum_order_qty,
        mnfctr_part_num,
        packaging_type_cd,
        cm_inv_org_key,
        vendor_prty_key,
        manufacturer_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        buy_sell_bu_id,
        src_rptd_buy_sell_bu_id,
        buy_sell_prdt_fmly_id,
        src_rptd_buy_sell_prdt_fmly_id,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_purchase_order_ln_subinv_ext') }}
),

final AS (
    SELECT
        purchase_order_line_key,
        edcs_num,
        product_key,
        program_type_cd,
        minimum_order_qty,
        mnfctr_part_num,
        packaging_type_cd,
        cm_inv_org_key,
        vendor_prty_key,
        manufacturer_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        buy_sell_bu_id,
        src_rptd_buy_sell_bu_id,
        buy_sell_prdt_fmly_id,
        src_rptd_buy_sell_prdt_fmly_id
    FROM source_w_purchase_order_ln_subinv_ext
)

SELECT * FROM final