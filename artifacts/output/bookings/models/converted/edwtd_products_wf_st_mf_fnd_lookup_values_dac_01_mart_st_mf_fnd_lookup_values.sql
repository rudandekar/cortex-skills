{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fnd_lookup_values_dac', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FND_LOOKUP_VALUES_DAC',
        'target_table': 'ST_MF_FND_LOOKUP_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.958316+00:00'
    }
) }}

WITH 

source_ff_mf_fnd_lookup_values AS (
    SELECT
        lookup_type,
        lang,
        security_group_id,
        lookup_code,
        meaning,
        view_application_id,
        global_name,
        enabled_flag,
        description,
        batch_id,
        create_datetime,
        ges_update_date,
        action_code,
        start_date_active,
        end_date_active,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        tag
    FROM {{ source('raw', 'ff_mf_fnd_lookup_values') }}
),

transformed_exptrans AS (
    SELECT
    lookup_type,
    lang,
    security_group_id,
    lookup_code,
    meaning,
    view_application_id,
    global_name,
    enabled_flag,
    description,
    batch_id,
    create_datetime,
    action_code,
    ges_update_date,
    start_date_active,
    end_date_active,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    tag
    FROM source_ff_mf_fnd_lookup_values
),

final AS (
    SELECT
        batch_id,
        lookup_type,
        lang,
        security_group_id,
        lookup_code,
        meaning,
        view_application_id,
        global_name,
        enabled_flag,
        description,
        start_date_active,
        end_date_active,
        create_datetime,
        ges_update_date,
        action_code,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        tag
    FROM transformed_exptrans
)

SELECT * FROM final