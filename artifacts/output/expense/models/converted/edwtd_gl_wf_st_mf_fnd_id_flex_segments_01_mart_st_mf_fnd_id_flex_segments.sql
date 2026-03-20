{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fnd_id_flex_segments', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FND_ID_FLEX_SEGMENTS',
        'target_table': 'ST_MF_FND_ID_FLEX_SEGMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.731906+00:00'
    }
) }}

WITH 

source_ff_mf_fnd_id_flex_segments AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        application_column_name,
        global_name,
        flex_value_set_id,
        segment_name,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_fnd_id_flex_segments') }}
),

final AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        application_column_name,
        global_name,
        flex_value_set_id,
        segment_name,
        ges_update_date,
        create_datetime,
        action_code
    FROM source_ff_mf_fnd_id_flex_segments
)

SELECT * FROM final