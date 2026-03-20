{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_consolidated_line_ref_numbers', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_ST_CONSOLIDATED_LINE_REF_NUMBERS',
        'target_table': 'ST_CONSOLIDATED_LINE_REF_NUMBERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.099771+00:00'
    }
) }}

WITH 

source_st_consolidated_line_ref_numbers AS (
    SELECT
        line_id,
        consolidated_line_ref_number,
        edw_create_dtm
    FROM {{ source('raw', 'st_consolidated_line_ref_numbers') }}
),

final AS (
    SELECT
        line_id,
        consolidated_line_ref_number,
        edw_create_dtm
    FROM source_st_consolidated_line_ref_numbers
)

SELECT * FROM final