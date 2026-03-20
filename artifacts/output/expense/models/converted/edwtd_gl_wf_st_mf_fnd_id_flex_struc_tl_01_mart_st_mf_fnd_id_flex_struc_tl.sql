{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fnd_id_flex_struc_tl', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FND_ID_FLEX_STRUC_TL',
        'target_table': 'ST_MF_FND_ID_FLEX_STRUC_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.953656+00:00'
    }
) }}

WITH 

source_ff_mf_fnd_id_flex_struc_tl AS (
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
    FROM {{ source('raw', 'ff_mf_fnd_id_flex_struc_tl') }}
),

final AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        language_code,
        global_name,
        id_flex_structure_name,
        description,
        ges_update_date,
        create_datetime,
        action_code
    FROM source_ff_mf_fnd_id_flex_struc_tl
)

SELECT * FROM final