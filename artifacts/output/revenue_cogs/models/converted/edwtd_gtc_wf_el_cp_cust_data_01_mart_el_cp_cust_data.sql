{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cp_cust_data', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CP_CUST_DATA',
        'target_table': 'EL_CP_CUST_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.880107+00:00'
    }
) }}

WITH 

source_el_cp_cust_data AS (
    SELECT
        cust_data_id,
        category_name,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute26,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code,
        source_deleted_flg
    FROM {{ source('raw', 'el_cp_cust_data') }}
),

final AS (
    SELECT
        cust_data_id,
        category_name,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute26,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code,
        source_deleted_flg
    FROM source_el_cp_cust_data
)

SELECT * FROM final