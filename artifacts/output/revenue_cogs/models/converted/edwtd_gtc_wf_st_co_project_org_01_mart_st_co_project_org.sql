{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_co_project_org', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CO_PROJECT_ORG',
        'target_table': 'ST_CO_PROJECT_ORG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.725788+00:00'
    }
) }}

WITH 

source_ff_co_project_org_incr AS (
    SELECT
        batch_id,
        project_org_id,
        project_id,
        org_id,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        org_type,
        org_mfr,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_co_project_org_incr') }}
),

final AS (
    SELECT
        batch_id,
        project_org_id,
        project_id,
        org_id,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        org_type,
        org_mfr,
        create_datetime,
        action_code
    FROM source_ff_co_project_org_incr
)

SELECT * FROM final