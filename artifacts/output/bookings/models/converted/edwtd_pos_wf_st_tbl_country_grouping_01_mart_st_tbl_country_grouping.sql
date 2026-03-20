{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tbl_country_grouping', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_TBL_COUNTRY_GROUPING',
        'target_table': 'ST_TBL_COUNTRY_GROUPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.756091+00:00'
    }
) }}

WITH 

source_ff_tbl_country_grouping AS (
    SELECT
        country,
        country_grouping_ecc,
        country_grouping,
        country_sub_group
    FROM {{ source('raw', 'ff_tbl_country_grouping') }}
),

final AS (
    SELECT
        country,
        country_grouping_ecc,
        country_grouping,
        country_sub_group
    FROM source_ff_tbl_country_grouping
)

SELECT * FROM final