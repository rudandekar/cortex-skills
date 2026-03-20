{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_shr_master_versions_v_confirmed_fdrr', 'batch', 'edwtd_sales_hier'],
    meta={
        'source_workflow': 'wf_m_ST_SHR_MASTER_VERSIONS_V_CONFIRMED_FDRR',
        'target_table': 'ST_SHR_MASTER_VERSNS_V_FDRR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.728230+00:00'
    }
) }}

WITH 

source_ff_shr_master_versions_v_confirmed_fdrr AS (
    SELECT
        batch_id,
        version_id,
        version_number,
        version_name,
        effective_date,
        expiration_date,
        structure_type_id,
        structure_type_name,
        status_id,
        status_name,
        date_modified,
        status_effective_date,
        status_expiration_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_shr_master_versions_v_confirmed_fdrr') }}
),

final AS (
    SELECT
        batch_id,
        version_id,
        version_number,
        version_name,
        effective_date,
        expiration_date,
        structure_type_id,
        structure_type_name,
        status_id,
        status_name,
        date_modified,
        status_effective_date,
        status_expiration_date,
        create_datetime,
        action_code
    FROM source_ff_shr_master_versions_v_confirmed_fdrr
)

SELECT * FROM final