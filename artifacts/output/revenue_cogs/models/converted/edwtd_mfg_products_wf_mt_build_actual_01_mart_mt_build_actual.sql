{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_build_actual', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_MT_BUILD_ACTUAL',
        'target_table': 'MT_BUILD_ACTUAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.996845+00:00'
    }
) }}

WITH 

source_mt_build_actual AS (
    SELECT
        bk_product_family_id,
        inventory_organization_key,
        bk_fiscal_year_week_num_int,
        dv_build_actual_usd_amt,
        revenue_flag,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_build_actual') }}
),

final AS (
    SELECT
        bk_product_family_id,
        inventory_organization_key,
        bk_fiscal_year_week_num_int,
        dv_build_actual_usd_amt,
        revenue_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_build_actual
)

SELECT * FROM final