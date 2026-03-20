{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_cib_spd_attribute_lookup', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_CIB_SPD_ATTRIBUTE_LOOKUP',
        'target_table': 'CSF_CIB_SPD_ATTRIBUTE_LOOKUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.097023+00:00'
    }
) }}

WITH 

source_stg_cib_spd_attribute_lookup AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        lookup_id,
        lookup_type,
        lookup_code,
        lookup_meaning,
        lookup_desc,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        lookup_usage,
        gg_enqueue_time,
        gg_dequeue_time
    FROM {{ source('raw', 'stg_cib_spd_attribute_lookup') }}
),

source_csf_cib_spd_attribute_lookup AS (
    SELECT
        ges_update_date,
        lookup_id,
        lookup_type,
        lookup_code,
        lookup_meaning,
        lookup_desc,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        lookup_usage,
        gg_enqueue_time,
        gg_dequeue_time
    FROM {{ source('raw', 'csf_cib_spd_attribute_lookup') }}
),

transformed_exptrans AS (
    SELECT
    ges_update_date,
    lookup_id,
    lookup_type,
    lookup_code,
    lookup_meaning,
    lookup_desc,
    enabled_flag,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    lookup_usage,
    gg_enqueue_time,
    gg_dequeue_time,
    source_commit_time,
    'INSERT' AS source_dml_type
    FROM source_csf_cib_spd_attribute_lookup
),

final AS (
    SELECT
        source_dml_type,
        ges_update_date,
        refresh_datetime,
        lookup_id,
        lookup_type,
        lookup_code,
        lookup_meaning,
        lookup_desc,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        lookup_usage,
        gg_enqueue_time,
        gg_dequeue_time
    FROM transformed_exptrans
)

SELECT * FROM final