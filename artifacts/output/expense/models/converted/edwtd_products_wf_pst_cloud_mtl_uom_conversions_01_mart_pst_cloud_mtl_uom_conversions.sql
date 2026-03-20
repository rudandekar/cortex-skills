{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_mtl_uom_conversions', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_MTL_UOM_CONVERSIONS',
        'target_table': 'PST_CLOUD_MTL_UOM_CONVERSIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.742570+00:00'
    }
) }}

WITH 

source_cbm_inv_uom_conversions AS (
    SELECT
        object_version_number,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        uom_code,
        uom_class,
        disable_date,
        request_id,
        conversion_id,
        contained_uom_code,
        job_definition_name,
        inventory_item_id,
        job_definition_package,
        conversion_rate,
        default_conversion_flag,
        uom_length,
        uom_width,
        uom_height,
        dimension_uom,
        interior_unit_code,
        interior_unit_qty,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'cbm_inv_uom_conversions') }}
),

final AS (
    SELECT
        object_version_number,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        uom_code,
        uom_class,
        disable_date,
        request_id,
        conversion_id,
        contained_uom_code,
        job_definition_name,
        inventory_item_id,
        job_definition_package,
        conversion_rate,
        default_conversion_flag,
        uom_length,
        uom_width,
        uom_height,
        dimension_uom,
        interior_unit_code,
        interior_unit_qty,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM source_cbm_inv_uom_conversions
)

SELECT * FROM final