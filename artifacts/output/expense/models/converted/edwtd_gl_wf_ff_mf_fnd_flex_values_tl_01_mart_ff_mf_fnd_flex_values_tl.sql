{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_fnd_flex_values_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_FND_FLEX_VALUES_TL',
        'target_table': 'FF_MF_FND_FLEX_VALUES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.812915+00:00'
    }
) }}

WITH 

source_mf_fnd_flex_values_tl AS (
    SELECT
        created_by,
        creation_date,
        description,
        flex_value_id,
        flex_value_meaning,
        ges_update_date,
        global_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_lang,
        cms_replication_date,
        cms_replication_number
    FROM {{ source('raw', 'mf_fnd_flex_values_tl') }}
),

transformed_exptrans AS (
    SELECT
    created_by,
    creation_date,
    description,
    flex_value_id,
    flex_value_meaning,
    ges_update_date,
    global_name,
    language,
    last_updated_by,
    last_update_date,
    last_update_login,
    source_lang,
    cms_replication_date,
    cms_replication_number,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_fnd_flex_values_tl
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        description,
        flex_value_id,
        flex_value_meaning,
        ges_update_date,
        global_name,
        language1,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_lang,
        cms_replication_date,
        cms_replication_number,
        create_datetime,
        action_code
    FROM transformed_exptrans
)

SELECT * FROM final