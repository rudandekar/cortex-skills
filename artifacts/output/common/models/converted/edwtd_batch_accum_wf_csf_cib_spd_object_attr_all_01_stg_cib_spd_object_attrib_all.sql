{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_cib_spd_object_attrib_all', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_CIB_SPD_OBJECT_ATTRIB_ALL',
        'target_table': 'STG_CIB_SPD_OBJECT_ATTRIB_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.795243+00:00'
    }
) }}

WITH 

source_stg_cib_spd_object_attrib_all AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        object_attribute_id,
        attribute_id,
        attribute_name,
        attribute_value,
        object_type,
        object_id,
        object_name,
        start_date_active,
        end_date_active,
        organization_id,
        cascade_enabled,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute_access_role,
        cascade_from_object_name,
        comments,
        cascade_from_object_type,
        gg_enqueue_time,
        gg_dequeue_time
    FROM {{ source('raw', 'stg_cib_spd_object_attrib_all') }}
),

source_csf_cib_spd_object_attrib_all AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        object_attribute_id,
        attribute_id,
        attribute_name,
        attribute_value,
        object_type,
        object_id,
        object_name,
        start_date_active,
        end_date_active,
        organization_id,
        cascade_enabled,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute_access_role,
        cascade_from_object_name,
        comments,
        cascade_from_object_type,
        gg_enqueue_time,
        gg_dequeue_time
    FROM {{ source('raw', 'csf_cib_spd_object_attrib_all') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    object_attribute_id,
    attribute_id,
    attribute_name,
    attribute_value,
    object_type,
    object_id,
    object_name,
    start_date_active,
    end_date_active,
    organization_id,
    cascade_enabled,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    attribute_access_role,
    cascade_from_object_name,
    comments,
    cascade_from_object_type,
    gg_enqueue_time,
    gg_dequeue_time
    FROM source_csf_cib_spd_object_attrib_all
),

final AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        object_attribute_id,
        attribute_id,
        attribute_name,
        attribute_value,
        object_type,
        object_id,
        object_name,
        start_date_active,
        end_date_active,
        organization_id,
        cascade_enabled,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute_access_role,
        cascade_from_object_name,
        comments,
        cascade_from_object_type,
        gg_enqueue_time,
        gg_dequeue_time
    FROM transformed_exptrans
)

SELECT * FROM final