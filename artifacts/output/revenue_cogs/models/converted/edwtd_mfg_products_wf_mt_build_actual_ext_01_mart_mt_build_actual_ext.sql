{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_build_actual_ext', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_MT_BUILD_ACTUAL_EXT',
        'target_table': 'MT_BUILD_ACTUAL_EXT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.342483+00:00'
    }
) }}

WITH 

source_mt_build_actual_ext AS (
    SELECT
        sales_order_line_key,
        inventory_organization_key,
        fiscal_year_week_num_int,
        revenue_flg,
        product_key,
        sales_territory_key,
        dv_build_actual_usd_amt,
        dv_delivery_type_cd,
        dv_tier_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_build_actual_ext') }}
),

final AS (
    SELECT
        sales_order_line_key,
        inventory_organization_key,
        fiscal_year_week_num_int,
        revenue_flg,
        product_key,
        sales_territory_key,
        dv_build_actual_usd_amt,
        dv_delivery_type_cd,
        dv_tier_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_build_actual_ext
)

SELECT * FROM final