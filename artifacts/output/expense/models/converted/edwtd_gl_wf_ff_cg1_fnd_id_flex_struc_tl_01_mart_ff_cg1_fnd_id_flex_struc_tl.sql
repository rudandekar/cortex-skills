{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cg1_fnd_id_flex_struc_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_CG1_FND_ID_FLEX_STRUC_TL',
        'target_table': 'FF_CG1_FND_ID_FLEX_STRUC_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.160770+00:00'
    }
) }}

WITH 

source_cg1_fnd_id_flex_struc_tl AS (
    SELECT
        application_id,
        id_flex_code,
        id_flex_num,
        language,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        id_flex_structure_name,
        source_lang,
        last_update_login,
        description,
        shorthand_prompt,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_fnd_id_flex_struc_tl') }}
),

transformed_exp_cg1_fnd_id_flex_struc_tl AS (
    SELECT
    application_id,
    id_flex_code,
    id_flex_num,
    language,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    id_flex_structure_name,
    source_lang,
    last_update_login,
    description,
    shorthand_prompt,
    ges_update_date,
    global_name,
    'BatchID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cg1_fnd_id_flex_struc_tl
),

final AS (
    SELECT
        application_id,
        id_flex_code,
        id_flex_num,
        language,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        id_flex_structure_name,
        source_lang,
        last_update_login,
        description,
        shorthand_prompt,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_cg1_fnd_id_flex_struc_tl
)

SELECT * FROM final