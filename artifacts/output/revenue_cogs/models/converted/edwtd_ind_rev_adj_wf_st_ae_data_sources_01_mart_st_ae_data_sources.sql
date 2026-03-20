{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_data_sources', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_ST_AE_DATA_SOURCES',
        'target_table': 'ST_AE_DATA_SOURCES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.436884+00:00'
    }
) }}

WITH 

source_st_ae_data_sources AS (
    SELECT
        batch_id,
        source_system_id,
        source_system_name,
        source_system_type_code,
        source_system_category_label,
        status,
        create_user,
        update_user,
        update_datetime,
        create_datetime,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ae_data_sources') }}
),

final AS (
    SELECT
        batch_id,
        source_system_id,
        source_system_name,
        source_system_type_code,
        source_system_category_label,
        status,
        create_user,
        update_user,
        update_datetime,
        create_datetime,
        create_timestamp,
        action_code
    FROM source_st_ae_data_sources
)

SELECT * FROM final