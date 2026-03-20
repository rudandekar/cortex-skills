{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ngccrm_lookup_details', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_NGCCRM_LOOKUP_DETAILS',
        'target_table': 'ST_NGCCRM_LOOKUP_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.470703+00:00'
    }
) }}

WITH 

source_ff_ngccrm_lookup_details AS (
    SELECT
        batch_id,
        details_id,
        lookup_id,
        detail_code,
        detail_meaning,
        detail_desc,
        enabled_flag,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'ff_ngccrm_lookup_details') }}
),

final AS (
    SELECT
        batch_id,
        details_id,
        lookup_id,
        detail_code,
        detail_meaning,
        detail_desc,
        enabled_flag,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        create_timestamp,
        action_code
    FROM source_ff_ngccrm_lookup_details
)

SELECT * FROM final