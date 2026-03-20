{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcp_table_set_ctl_cg1', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCP_TABLE_SET_CTL_CG1',
        'target_table': 'ST_XXCP_TABLE_SET_CTL_CG1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.207970+00:00'
    }
) }}

WITH 

source_cg1_xxcp_table_set_ctl AS (
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
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_xxcp_table_set_ctl') }}
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
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM source_cg1_xxcp_table_set_ctl
)

SELECT * FROM final