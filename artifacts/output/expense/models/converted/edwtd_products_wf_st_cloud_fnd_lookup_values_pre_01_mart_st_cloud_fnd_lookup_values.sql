{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cloud_fnd_lookup_values_pre', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_ST_CLOUD_FND_LOOKUP_VALUES_PRE',
        'target_table': 'ST_CLOUD_FND_LOOKUP_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.779601+00:00'
    }
) }}

WITH 

source_pst_cloud_fnd_lookup_values AS (
    SELECT
        creation_date,
        description,
        enabled_flag,
        end_date_active,
        last_update_date,
        lookup_code,
        lookup_type,
        meaning,
        set_id,
        start_date_active,
        tag,
        view_application_id,
        source_lang,
        language,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'pst_cloud_fnd_lookup_values') }}
),

final AS (
    SELECT
        lookup_type,
        lang,
        lookup_code,
        meaning,
        description,
        enabled_flag,
        start_date_active,
        end_date_active,
        security_group_id,
        view_application_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        ges_update_date,
        create_datetime,
        action_code,
        attribute15,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cloud_fnd_lookup_values
)

SELECT * FROM final