{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_ra_batch_sources_all', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_RA_BATCH_SOURCES_ALL',
        'target_table': 'ST_OOD_FUSN_RA_BATCH_SOURCES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.884432+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_ra_batch_sources_all AS (
    SELECT
        batch_source_id,
        batch_source_type,
        description,
        end_date,
        name,
        org_id,
        start_date,
        status,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_ra_batch_sources_all') }}
),

final AS (
    SELECT
        batch_source_id,
        batch_source_type,
        description,
        end_date,
        name,
        org_id,
        start_date,
        status,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_ra_batch_sources_all
)

SELECT * FROM final