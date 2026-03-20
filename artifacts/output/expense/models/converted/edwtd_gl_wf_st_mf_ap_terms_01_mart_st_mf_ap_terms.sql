{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_ap_terms', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_AP_TERMS',
        'target_table': 'ST_MF_AP_TERMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.642704+00:00'
    }
) }}

WITH 

source_ff_mf_ap_terms AS (
    SELECT
        description,
        ges_update_date,
        global_name,
        name,
        term_id
    FROM {{ source('raw', 'ff_mf_ap_terms') }}
),

final AS (
    SELECT
        description,
        ges_update_date,
        global_name,
        name,
        term_id
    FROM source_ff_mf_ap_terms
)

SELECT * FROM final