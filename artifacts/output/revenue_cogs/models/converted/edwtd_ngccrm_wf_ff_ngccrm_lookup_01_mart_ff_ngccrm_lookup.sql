{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ngccrm_lookup', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_NGCCRM_LOOKUP',
        'target_table': 'FF_NGCCRM_LOOKUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.984429+00:00'
    }
) }}

WITH 

source_ngccrm_lookup AS (
    SELECT
        lookup_id,
        lookup_type,
        lookup_meaning,
        lookup_desc,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        lookup_category
    FROM {{ source('raw', 'ngccrm_lookup') }}
),

transformed_exp_ngccrm_lookup AS (
    SELECT
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
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_ngccrm_lookup
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
    FROM transformed_exp_ngccrm_lookup
)

SELECT * FROM final