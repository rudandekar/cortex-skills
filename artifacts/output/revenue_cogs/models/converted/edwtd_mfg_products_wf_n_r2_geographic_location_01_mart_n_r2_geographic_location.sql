{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_r2_geographic_location', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_R2_GEOGRAPHIC_LOCATION',
        'target_table': 'N_R2_GEOGRAPHIC_LOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.022394+00:00'
    }
) }}

WITH 

source_w_r2_geographic_location AS (
    SELECT
        bk_geographic_location_cd,
        geographic_region_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_r2_geographic_location') }}
),

final AS (
    SELECT
        bk_geographic_location_cd,
        geographic_region_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_r2_geographic_location
)

SELECT * FROM final