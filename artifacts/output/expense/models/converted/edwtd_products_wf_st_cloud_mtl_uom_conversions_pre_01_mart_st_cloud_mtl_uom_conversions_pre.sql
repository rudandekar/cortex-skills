{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cloud_mtl_uom_conversions_pre', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_ST_CLOUD_MTL_UOM_CONVERSIONS_PRE',
        'target_table': 'ST_CLOUD_MTL_UOM_CONVERSIONS_PRE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.950537+00:00'
    }
) }}

WITH 

source_pst_cloud_mtl_uom_conversions AS (
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
    FROM {{ source('raw', 'pst_cloud_mtl_uom_conversions') }}
),

final AS (
    SELECT
        unit_of_measure,
        uom_code,
        uom_class,
        inventory_item_id,
        conversion_rate,
        default_conversion_flag,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        disable_date,
        request_id,
        uom_length,
        uom_width,
        uom_height,
        dimension_uom,
        source_dml_type,
        refresh_datetime,
        global_name,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cloud_mtl_uom_conversions
)

SELECT * FROM final