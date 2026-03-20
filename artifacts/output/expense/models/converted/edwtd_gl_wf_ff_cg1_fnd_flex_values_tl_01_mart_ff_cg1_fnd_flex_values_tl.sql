{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cg1_fnd_flex_values_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_CG1_FND_FLEX_VALUES_TL',
        'target_table': 'FF_CG1_FND_FLEX_VALUES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.730253+00:00'
    }
) }}

WITH 

source_cg1_fnd_flex_values_tl AS (
    SELECT
        flex_value_id,
        language,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        source_lang,
        flex_value_meaning,
        last_update_login,
        description,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_fnd_flex_values_tl') }}
),

transformed_ex_ff_cg1_fnd_flex_values_tl AS (
    SELECT
    flex_value_id,
    language,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    source_lang,
    flex_value_meaning,
    last_update_login,
    description,
    ges_update_date,
    global_name,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_cg1_fnd_flex_values_tl
),

final AS (
    SELECT
        flex_value_id,
        language,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        source_lang,
        flex_value_meaning,
        last_update_login,
        description,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_ex_ff_cg1_fnd_flex_values_tl
)

SELECT * FROM final