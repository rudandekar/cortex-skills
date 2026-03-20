{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_fnd_lookup_values', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_FND_LOOKUP_VALUES',
        'target_table': 'PST_CLOUD_FND_LOOKUP_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.087799+00:00'
    }
) }}

WITH 

source_cbm_fnd_lookup_values AS (
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
    FROM {{ source('raw', 'cbm_fnd_lookup_values') }}
),

transformed_exp_cloud_fnd_lookup_values AS (
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
    record_count,
    SUBSTR(CREATION_DATE,1,19) AS creation_date_new,
    SUBSTR(LAST_UPDATE_DATE,1,19) AS last_update_date_new,
    IFF(ISNULL(VIEW_APPLICATION_ID),0,VIEW_APPLICATION_ID) AS view_application_id_new
    FROM source_cbm_fnd_lookup_values
),

final AS (
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
        language1,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM transformed_exp_cloud_fnd_lookup_values
)

SELECT * FROM final