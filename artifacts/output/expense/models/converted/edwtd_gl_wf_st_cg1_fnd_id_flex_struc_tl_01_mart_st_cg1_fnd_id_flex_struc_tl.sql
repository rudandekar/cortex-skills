{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_fnd_id_flex_struc_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_FND_ID_FLEX_STRUC_TL',
        'target_table': 'ST_CG1_FND_ID_FLEX_STRUC_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.596013+00:00'
    }
) }}

WITH 

source_ff_cg1_fnd_id_flex_struc_tl AS (
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
    FROM {{ source('raw', 'ff_cg1_fnd_id_flex_struc_tl') }}
),

final AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        language_code,
        last_update_date,
        last_updated_by,
        global_name,
        id_flex_structure_name,
        description,
        creation_date,
        created_by,
        last_update_login,
        shorthand_prompt,
        source_lang,
        ges_update_date,
        create_datetime,
        action_code
    FROM source_ff_cg1_fnd_id_flex_struc_tl
)

SELECT * FROM final