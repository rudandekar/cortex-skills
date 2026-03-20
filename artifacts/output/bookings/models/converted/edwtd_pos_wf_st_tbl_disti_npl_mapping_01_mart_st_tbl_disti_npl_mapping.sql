{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tbl_disti_npl_mapping', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_TBL_DISTI_NPL_MAPPING',
        'target_table': 'ST_TBL_DISTI_NPL_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.741819+00:00'
    }
) }}

WITH 

source_ff_tbl_disti_npl_mapping AS (
    SELECT
        fiscal_quarter_id,
        country,
        be_geo_id,
        profile_id,
        npl_flag
    FROM {{ source('raw', 'ff_tbl_disti_npl_mapping') }}
),

final AS (
    SELECT
        fiscal_quarter_id,
        country,
        be_geo_id,
        profile_id,
        npl_flag
    FROM source_ff_tbl_disti_npl_mapping
)

SELECT * FROM final