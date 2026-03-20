{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cp_table_set_ctl_bv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CP_TABLE_SET_CTL_BV',
        'target_table': 'ST_CP_TABLE_SET_CTL_BV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.463948+00:00'
    }
) }}

WITH 

source_bv_cp_table_set_ctl AS (
    SELECT
        set_id,
        set_base_table,
        source_id,
        set_type,
        set_name,
        attribute_set_id,
        enabled_flag,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'bv_cp_table_set_ctl') }}
),

final AS (
    SELECT
        set_id,
        set_base_table,
        source_id,
        set_type,
        set_name,
        attribute_set_id,
        enabled_flag,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_bv_cp_table_set_ctl
)

SELECT * FROM final