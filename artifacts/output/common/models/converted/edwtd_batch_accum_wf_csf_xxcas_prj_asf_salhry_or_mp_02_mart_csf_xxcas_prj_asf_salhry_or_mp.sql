{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_xxcas_prj_asf_salhry_or_mp', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_ASF_SALHRY_OR_MP',
        'target_table': 'CSF_XXCAS_PRJ_ASF_SALHRY_OR_MP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.613884+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_asf_salhry_or_mp AS (
    SELECT
        map_id_seq,
        erp_sales_hierarchy_level_1,
        erp_sales_hierarchy_level_2,
        erp_sales_hierarchy_level_3,
        erp_sales_hierarchy_level_4,
        erp_sales_hierarchy_level_5,
        erp_sales_hierarchy_level_6,
        node_sales_hierarchy_level_1,
        node_sales_hierarchy_level_2,
        node_sales_hierarchy_level_3,
        node_sales_hierarchy_level_4,
        node_sales_hierarchy_level_5,
        node_sales_hierarchy_level_6,
        version_id,
        version_number,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        effective_start_date,
        effective_end_date,
        prj_owning_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_asf_salhry_or_mp') }}
),

source_csf_xxcas_prj_asf_slehr_org_mp AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        map_id_seq,
        erp_sales_hierarchy_level_1,
        erp_sales_hierarchy_level_2,
        erp_sales_hierarchy_level_3,
        erp_sales_hierarchy_level_4,
        erp_sales_hierarchy_level_5,
        erp_sales_hierarchy_level_6,
        node_sales_hierarchy_level_1,
        node_sales_hierarchy_level_2,
        node_sales_hierarchy_level_3,
        node_sales_hierarchy_level_4,
        node_sales_hierarchy_level_5,
        node_sales_hierarchy_level_6,
        version_id,
        version_number,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        effective_start_date,
        effective_end_date,
        prj_owning_id
    FROM {{ source('raw', 'csf_xxcas_prj_asf_slehr_org_mp') }}
),

transformed_exp_csf_xxcas_prj_asf_salhry_or_mp AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    map_id_seq,
    erp_sales_hierarchy_level_1,
    erp_sales_hierarchy_level_2,
    erp_sales_hierarchy_level_3,
    erp_sales_hierarchy_level_4,
    erp_sales_hierarchy_level_5,
    erp_sales_hierarchy_level_6,
    node_sales_hierarchy_level_1,
    node_sales_hierarchy_level_2,
    node_sales_hierarchy_level_3,
    node_sales_hierarchy_level_4,
    node_sales_hierarchy_level_5,
    node_sales_hierarchy_level_6,
    version_id,
    version_number,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    effective_start_date,
    effective_end_date,
    prj_owning_id
    FROM source_csf_xxcas_prj_asf_slehr_org_mp
),

final AS (
    SELECT
        map_id_seq,
        erp_sales_hierarchy_level_1,
        erp_sales_hierarchy_level_2,
        erp_sales_hierarchy_level_3,
        erp_sales_hierarchy_level_4,
        erp_sales_hierarchy_level_5,
        erp_sales_hierarchy_level_6,
        node_sales_hierarchy_level_1,
        node_sales_hierarchy_level_2,
        node_sales_hierarchy_level_3,
        node_sales_hierarchy_level_4,
        node_sales_hierarchy_level_5,
        node_sales_hierarchy_level_6,
        version_id,
        version_number,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        effective_start_date,
        effective_end_date,
        prj_owning_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_asf_salhry_or_mp
)

SELECT * FROM final