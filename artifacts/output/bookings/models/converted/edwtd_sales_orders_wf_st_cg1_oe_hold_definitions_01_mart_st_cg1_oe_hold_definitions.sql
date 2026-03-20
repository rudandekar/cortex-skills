{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_oe_hold_definitions', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_OE_HOLD_DEFINITIONS',
        'target_table': 'ST_CG1_OE_HOLD_DEFINITIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.075468+00:00'
    }
) }}

WITH 

source_cg1_oe_hold_definitions AS (
    SELECT
        hold_id,
        activity_name,
        name,
        attribute2,
        description,
        type_code,
        attribute6,
        apply_to_order_and_line_flag,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute3,
        attribute4,
        attribute5,
        attribute7,
        attribute8,
        attribute9,
        context,
        created_by,
        creation_date,
        end_date_active,
        hold_included_items_flag,
        item_type,
        last_update_date,
        last_update_login,
        last_updated_by,
        progress_wf_on_release_flag,
        start_date_active,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_oe_hold_definitions') }}
),

final AS (
    SELECT
        hold_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        name,
        type_code,
        last_update_login,
        description,
        start_date_active,
        end_date_active,
        item_type,
        activity_name,
        context,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        hold_included_items_flag,
        apply_to_order_and_line_flag,
        progress_wf_on_release_flag,
        source_commit_time,
        global_name,
        create_datetime,
        source_dml_type,
        refresh_datetime
    FROM source_cg1_oe_hold_definitions
)

SELECT * FROM final