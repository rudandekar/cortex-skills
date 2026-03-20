{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ngccrm_lookup_details', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_NGCCRM_LOOKUP_DETAILS',
        'target_table': 'FF_NGCCRM_LOOKUP_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.287184+00:00'
    }
) }}

WITH 

source_ngccrm_lookup_details AS (
    SELECT
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
        last_updated_date
    FROM {{ source('raw', 'ngccrm_lookup_details') }}
),

transformed_exp_ngccrm_lookup_details AS (
    SELECT
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
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_ngccrm_lookup_details
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
    FROM transformed_exp_ngccrm_lookup_details
)

SELECT * FROM final