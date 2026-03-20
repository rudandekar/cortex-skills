{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_ra_terms_tl', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_RA_TERMS_TL',
        'target_table': 'ST_OOD_FUSN_RA_TERMS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.787268+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_ra_terms_tl AS (
    SELECT
        name,
        description,
        term_id,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_ra_terms_tl') }}
),

final AS (
    SELECT
        name,
        description,
        term_id,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_ra_terms_tl
)

SELECT * FROM final