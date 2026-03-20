{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_oe_order_sources', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_OE_ORDER_SOURCES',
        'target_table': 'STG_CG1_OE_ORDER_SOURCES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.610652+00:00'
    }
) }}

WITH 

source_stg_cg1_oe_order_sources AS (
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
    FROM {{ source('raw', 'stg_cg1_oe_order_sources') }}
),

source_cg1_oe_order_sources AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
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
        aia_enabled_flag
    FROM {{ source('raw', 'cg1_oe_order_sources') }}
),

transformed_exp_cg1_oe_order_sources AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
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
    aia_enabled_flag
    FROM source_cg1_oe_order_sources
),

final AS (
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
    FROM transformed_exp_cg1_oe_order_sources
)

SELECT * FROM final