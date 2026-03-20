{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_ra_rules', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_RA_RULES',
        'target_table': 'ST_OOD_FUSN_RA_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.339855+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_ra_rules AS (
    SELECT
        description,
        name,
        rule_id,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_ra_rules') }}
),

final AS (
    SELECT
        description,
        name,
        rule_id,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_ra_rules
)

SELECT * FROM final