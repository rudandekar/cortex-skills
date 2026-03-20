{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_mtl_uom_conversions', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_MTL_UOM_CONVERSIONS',
        'target_table': 'CG1_MTL_UOM_CONVERSIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.633075+00:00'
    }
) }}

WITH 

source_cg1_mtl_uom_conversions AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
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
        program_application_id,
        program_id,
        program_update_date,
        length,
        width,
        height,
        dimension_uom
    FROM {{ source('raw', 'cg1_mtl_uom_conversions') }}
),

source_stg_cg1_mtl_uom_conversions AS (
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
        program_application_id,
        program_id,
        program_update_date,
        length,
        width,
        height,
        dimension_uom,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_cg1_mtl_uom_conversions') }}
),

transformed_exp_cg1_mtl_uom_conversions AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
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
    program_application_id,
    program_id,
    program_update_date,
    length,
    width,
    height,
    dimension_uom
    FROM source_stg_cg1_mtl_uom_conversions
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
        program_application_id,
        program_id,
        program_update_date,
        length,
        width,
        height,
        dimension_uom,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_cg1_mtl_uom_conversions
)

SELECT * FROM final