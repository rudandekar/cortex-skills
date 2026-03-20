{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_oe_order_sources', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_OE_ORDER_SOURCES',
        'target_table': 'ST_CG1_OE_ORDER_SOURCES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.257080+00:00'
    }
) }}

WITH 

source_cg1_oe_order_sources AS (
    SELECT
        order_source_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        name,
        description,
        enabled_flag,
        create_customers_flag,
        use_ids_flag,
        aia_enabled_flag,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_oe_order_sources') }}
),

final AS (
    SELECT
        creation_date,
        description,
        enabled_flag,
        name,
        order_source_id,
        source_commit_time,
        global_name
    FROM source_cg1_oe_order_sources
)

SELECT * FROM final