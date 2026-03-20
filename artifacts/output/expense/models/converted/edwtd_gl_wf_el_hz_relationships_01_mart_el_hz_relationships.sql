{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_hz_relationships', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_HZ_RELATIONSHIPS',
        'target_table': 'EL_HZ_RELATIONSHIPS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.171061+00:00'
    }
) }}

WITH 

source_st_mf_hz_relationships_ap AS (
    SELECT
        batch_id,
        created_by,
        created_by_module,
        creation_date,
        directional_flag,
        direction_code,
        end_date,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        object_id,
        object_table_name,
        object_type,
        object_version_number,
        party_id,
        relationship_code,
        relationship_id,
        relationship_type,
        request_id,
        start_date,
        status,
        subject_id,
        subject_table_name,
        subject_type,
        creation_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_hz_relationships_ap') }}
),

final AS (
    SELECT
        created_by,
        created_by_module,
        creation_date,
        directional_flag,
        direction_code,
        end_date,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        object_id,
        object_table_name,
        object_type,
        object_version_number,
        party_id,
        relationship_code,
        relationship_id,
        relationship_type,
        request_id,
        start_date,
        status,
        subject_id,
        subject_table_name,
        subject_type,
        creation_datetime
    FROM source_st_mf_hz_relationships_ap
)

SELECT * FROM final