{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_alloc_prcnt_sbase', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_R_ALLOC_PRCNT_SBASE',
        'target_table': 'MT_ALLOC_PRCNT_SBASE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.764587+00:00'
    }
) }}

WITH 

source_mt_alloc_prcnt_sbase AS (
    SELECT
        item_key,
        ru_bk_product_subgroup_id,
        ru_bk_product_family_id,
        bk_product_type_id,
        bk_technology_group_id,
        bk_business_unit_id,
        dollar_allocation_pct,
        dv_actual_unit_aloc_pct,
        l4_technology_mkt_segment_name,
        hier_type
    FROM {{ source('raw', 'mt_alloc_prcnt_sbase') }}
),

final AS (
    SELECT
        item_key,
        ru_bk_product_subgroup_id,
        ru_bk_product_family_id,
        bk_product_type_id,
        bk_technology_group_id,
        bk_business_unit_id,
        dollar_allocation_pct,
        dv_actual_unit_aloc_pct,
        l4_technology_mkt_segment_name,
        hier_type
    FROM source_mt_alloc_prcnt_sbase
)

SELECT * FROM final