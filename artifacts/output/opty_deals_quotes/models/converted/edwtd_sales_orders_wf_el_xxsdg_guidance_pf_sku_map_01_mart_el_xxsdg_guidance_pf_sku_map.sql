{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxsdg_guidance_pf_sku_map', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_XXSDG_GUIDANCE_PF_SKU_MAP',
        'target_table': 'EL_XXSDG_GUIDANCE_PF_SKU_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.958354+00:00'
    }
) }}

WITH 

source_el_xxsdg_guidance_pf_sku_map AS (
    SELECT
        request_id,
        primary_product_family,
        line_object_id,
        part_number,
        cmrs_major_product_family,
        created_on,
        updated_on,
        guidance_id,
        update_datetime
    FROM {{ source('raw', 'el_xxsdg_guidance_pf_sku_map') }}
),

final AS (
    SELECT
        request_id,
        primary_product_family,
        line_object_id,
        part_number,
        cmrs_major_product_family,
        created_on,
        updated_on,
        guidance_id,
        update_datetime
    FROM source_el_xxsdg_guidance_pf_sku_map
)

SELECT * FROM final