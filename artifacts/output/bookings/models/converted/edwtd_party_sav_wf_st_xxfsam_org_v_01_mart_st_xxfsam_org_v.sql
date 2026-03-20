{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_org_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_ORG_V',
        'target_table': 'ST_XXFSAM_ORG_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.962473+00:00'
    }
) }}

WITH 

source_ff_xxfsam_org_v AS (
    SELECT
        organization_id,
        organization_name,
        start_date,
        end_date,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by
    FROM {{ source('raw', 'ff_xxfsam_org_v') }}
),

final AS (
    SELECT
        organization_id,
        organization_name,
        start_date,
        end_date,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by
    FROM source_ff_xxfsam_org_v
)

SELECT * FROM final