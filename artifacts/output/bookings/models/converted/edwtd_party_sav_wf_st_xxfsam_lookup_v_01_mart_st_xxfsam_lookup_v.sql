{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_lookup_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_LOOKUP_V',
        'target_table': 'ST_XXFSAM_LOOKUP_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.286397+00:00'
    }
) }}

WITH 

source_ff_xxfsam_lookup_v AS (
    SELECT
        lookup_id,
        lookup_type,
        lookup_description,
        lookup_value,
        org_id,
        start_date,
        end_date,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by
    FROM {{ source('raw', 'ff_xxfsam_lookup_v') }}
),

final AS (
    SELECT
        lookup_id,
        lookup_type,
        lookup_description,
        lookup_value,
        org_id,
        start_date,
        end_date,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by
    FROM source_ff_xxfsam_lookup_v
)

SELECT * FROM final