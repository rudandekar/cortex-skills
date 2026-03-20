{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxsdg_guidance_pf_sku_map', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_FF_XXSDG_GUIDANCE_PF_SKU_MAP',
        'target_table': 'FF_XXSDG_GUIDANCE_PF_SKU_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.881511+00:00'
    }
) }}

WITH 

source_xxsdg_guidance_pf_sku_map AS (
    SELECT
        request_id,
        primary_product_family,
        line_object_id,
        line_number,
        line_type_id,
        part_number,
        original_product_family,
        cmrs_major_product_family,
        created_on,
        updated_on,
        guidance_id,
        additional_credit,
        config_bundle_disc,
        discount_amts,
        discount_names,
        discount_types,
        extended_list_price,
        final_net_price,
        parent_line_object_id,
        quantity,
        service_duration,
        tradein,
        is_xaas_sku,
        bundle_object_id,
        part_category,
        ext_overhead_cost,
        ext_standard_cost,
        ext_royalty_cost,
        unit_overhead_cost,
        unit_standard_cost,
        unit_royalty_cost
    FROM {{ source('raw', 'xxsdg_guidance_pf_sku_map') }}
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
        guidance_id
    FROM source_xxsdg_guidance_pf_sku_map
)

SELECT * FROM final