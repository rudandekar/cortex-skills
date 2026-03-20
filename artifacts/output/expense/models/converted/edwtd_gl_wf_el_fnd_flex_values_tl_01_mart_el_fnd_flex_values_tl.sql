{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fnd_flex_values_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_FND_FLEX_VALUES_TL',
        'target_table': 'EL_FND_FLEX_VALUES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.624572+00:00'
    }
) }}

WITH 

source_st_mf_fnd_flex_values_tl AS (
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
    FROM {{ source('raw', 'st_mf_fnd_flex_values_tl') }}
),

final AS (
    SELECT
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_mf_fnd_flex_values_tl
)

SELECT * FROM final