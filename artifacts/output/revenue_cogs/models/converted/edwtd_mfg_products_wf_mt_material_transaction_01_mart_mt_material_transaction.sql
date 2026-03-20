{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_material_transaction', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_MT_MATERIAL_TRANSACTION',
        'target_table': 'MT_MATERIAL_TRANSACTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.255811+00:00'
    }
) }}

WITH 

source_mt_material_transaction AS (
    SELECT
        material_transaction_key,
        sales_order_line_key,
        ru_parnt_sls_order_line_key,
        sales_order_key,
        bk_transaction_type_name,
        product_key,
        item_type_cd,
        inventory_organization_key,
        transactional_currency_cd,
        conversion_rt,
        bk_option_num_int,
        transaction_dtm,
        source_create_dtm,
        dv_source_create_dt,
        build_dtm,
        rtv_dtm,
        build_dt,
        rtv_dt,
        order_qty,
        build_qty,
        rtv_qty,
        unit_sale_price_amt,
        build_amt,
        rtv_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_material_transaction') }}
),

final AS (
    SELECT
        material_transaction_key,
        sales_order_line_key,
        ru_parnt_sls_order_line_key,
        sales_order_key,
        bk_transaction_type_name,
        product_key,
        item_type_cd,
        inventory_organization_key,
        transactional_currency_cd,
        conversion_rt,
        bk_option_num_int,
        transaction_dtm,
        source_create_dtm,
        dv_source_create_dt,
        build_dtm,
        rtv_dtm,
        build_dt,
        rtv_dt,
        order_qty,
        build_qty,
        rtv_qty,
        unit_sale_price_amt,
        build_amt,
        rtv_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        transaction_qty,
        dv_major_minor_line_qty
    FROM source_mt_material_transaction
)

SELECT * FROM final