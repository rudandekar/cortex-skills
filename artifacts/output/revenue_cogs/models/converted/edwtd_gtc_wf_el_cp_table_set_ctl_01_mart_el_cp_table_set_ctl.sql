{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cp_table_set_ctl', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CP_TABLE_SET_CTL',
        'target_table': 'EL_CP_TABLE_SET_CTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.177541+00:00'
    }
) }}

WITH 

source_el_cp_table_set_ctl AS (
    SELECT
        set_id,
        set_base_table,
        set_name,
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'el_cp_table_set_ctl') }}
),

final AS (
    SELECT
        set_id,
        set_base_table,
        set_name,
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_el_cp_table_set_ctl
)

SELECT * FROM final