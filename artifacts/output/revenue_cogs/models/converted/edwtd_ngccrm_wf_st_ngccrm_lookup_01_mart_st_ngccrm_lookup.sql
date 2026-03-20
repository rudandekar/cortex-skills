{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ngccrm_lookup', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_NGCCRM_LOOKUP',
        'target_table': 'ST_NGCCRM_LOOKUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.014356+00:00'
    }
) }}

WITH 

source_ff_ngccrm_lookup AS (
    SELECT
        batch_id,
        lookup_id,
        lookup_type,
        lookup_meaning,
        lookup_desc,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        lookup_category,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'ff_ngccrm_lookup') }}
),

final AS (
    SELECT
        batch_id,
        lookup_id,
        lookup_type,
        lookup_meaning,
        lookup_desc,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        lookup_category,
        create_timestamp,
        action_code
    FROM source_ff_ngccrm_lookup
)

SELECT * FROM final