{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_co_project_org', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_CO_PROJECT_ORG',
        'target_table': 'FF_CO_PROJECT_ORG_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.207587+00:00'
    }
) }}

WITH 

source_co_project_org AS (
    SELECT
        project_org_id,
        project_id,
        org_id,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        org_type,
        org_mfr
    FROM {{ source('raw', 'co_project_org') }}
),

transformed_exptrans AS (
    SELECT
    project_org_id,
    project_id,
    org_id,
    created_by,
    created_date,
    last_updated_by,
    last_updated_date,
    org_type,
    org_mfr,
    'BATCHID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_co_project_org
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
    FROM transformed_exptrans
)

SELECT * FROM final