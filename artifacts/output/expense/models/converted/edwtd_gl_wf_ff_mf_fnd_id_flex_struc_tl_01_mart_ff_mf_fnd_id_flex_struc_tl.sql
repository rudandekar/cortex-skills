{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_fnd_id_flex_struc_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_FND_ID_FLEX_STRUC_TL',
        'target_table': 'FF_MF_FND_ID_FLEX_STRUC_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.885738+00:00'
    }
) }}

WITH 

source_mf_fnd_id_flex_struc_tl AS (
    SELECT
        application_id,
        created_by,
        creation_date,
        description,
        ges_update_date,
        global_name,
        id_flex_code,
        id_flex_num,
        id_flex_structure_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        shorthand_prompt,
        source_lang
    FROM {{ source('raw', 'mf_fnd_id_flex_struc_tl') }}
),

transformed_exp_default_values AS (
    SELECT
    application_id,
    description,
    global_name,
    id_flex_code,
    id_flex_num,
    id_flex_structure_name,
    language,
    ges_update_date,
    'I' AS action_code,
    CURRENT_TIMESTAMP() AS create_datetime,
    'BatchId' AS batch_id
    FROM source_mf_fnd_id_flex_struc_tl
),

final AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        language,
        global_name,
        id_flex_structure_name,
        description,
        ges_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_default_values
)

SELECT * FROM final