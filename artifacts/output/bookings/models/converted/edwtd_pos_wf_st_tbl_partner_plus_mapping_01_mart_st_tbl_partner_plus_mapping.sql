{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tbl_partner_plus_mapping', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_TBL_PARTNER_PLUS_MAPPING',
        'target_table': 'ST_TBL_PARTNER_PLUS_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.242201+00:00'
    }
) }}

WITH 

source_ff_tbl_partner_plus_mapping AS (
    SELECT
        country,
        be_geo_id,
        tier,
        fiscal_quarter_id
    FROM {{ source('raw', 'ff_tbl_partner_plus_mapping') }}
),

final AS (
    SELECT
        country,
        be_geo_id,
        tier,
        fiscal_quarter_id
    FROM source_ff_tbl_partner_plus_mapping
)

SELECT * FROM final