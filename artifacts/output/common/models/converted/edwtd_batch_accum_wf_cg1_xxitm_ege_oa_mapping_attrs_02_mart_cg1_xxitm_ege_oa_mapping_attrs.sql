{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxitm_ege_oa_mapping_attrs', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXITM_EGE_OA_MAPPING_ATTRS',
        'target_table': 'CG1_XXITM_EGE_OA_MAPPING_ATTRS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.799444+00:00'
    }
) }}

WITH 

source_stg_cg1_xxitm_ege_oa_map_attrs AS (
    SELECT
        oa_mapping_id,
        attr_sku_name,
        attr_sku_id,
        attr_sku_org_id,
        parent_item_id,
        lp_split_percentage,
        mapping_start_date,
        mapping_end_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag,
        disable_flag,
        exception_flag
    FROM {{ source('raw', 'stg_cg1_xxitm_ege_oa_map_attrs') }}
),

source_cg1_xxitm_ege_oa_mapping_attrs AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        oa_mapping_id,
        attr_sku_name,
        attr_sku_id,
        attr_sku_org_id,
        parent_item_id,
        lp_split_percentage,
        mapping_start_date,
        mapping_end_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        exception_flag,
        disable_flag,
        multi_oa_bundle_enabled
    FROM {{ source('raw', 'cg1_xxitm_ege_oa_mapping_attrs') }}
),

transformed_exp_cg1_xxitm_ege_oa_mapping_attrs AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    oa_mapping_id,
    attr_sku_name,
    attr_sku_id,
    attr_sku_org_id,
    parent_item_id,
    lp_split_percentage,
    mapping_start_date,
    mapping_end_date,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    exception_flag,
    disable_flag
    FROM source_cg1_xxitm_ege_oa_mapping_attrs
),

final AS (
    SELECT
        oa_mapping_id,
        attr_sku_name,
        attr_sku_id,
        attr_sku_org_id,
        parent_item_id,
        lp_split_percentage,
        mapping_start_date,
        mapping_end_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag,
        disable_flag,
        exception_flag
    FROM transformed_exp_cg1_xxitm_ege_oa_mapping_attrs
)

SELECT * FROM final