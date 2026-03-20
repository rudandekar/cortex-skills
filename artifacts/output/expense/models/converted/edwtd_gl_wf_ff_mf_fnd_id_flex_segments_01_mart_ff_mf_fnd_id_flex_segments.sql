{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_fnd_id_flex_segments', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_FND_ID_FLEX_SEGMENTS',
        'target_table': 'FF_MF_FND_ID_FLEX_SEGMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.044650+00:00'
    }
) }}

WITH 

source_mf_fnd_id_flex_segments AS (
    SELECT
        application_column_index_flag,
        application_column_name,
        application_id,
        concatenation_description_len,
        created_by,
        creation_date,
        default_type,
        default_value,
        display_flag,
        display_size,
        enabled_flag,
        flex_value_set_id,
        ges_update_date,
        global_name,
        id_flex_code,
        id_flex_num,
        last_updated_by,
        last_update_date,
        last_update_login,
        maximum_description_len,
        range_code,
        required_flag,
        runtime_property_function,
        security_enabled_flag,
        segment_name,
        segment_num
    FROM {{ source('raw', 'mf_fnd_id_flex_segments') }}
),

transformed_exp_mf_fnd_id_flex_segments AS (
    SELECT
    application_id,
    id_flex_code,
    id_flex_num,
    application_column_name,
    global_name,
    flex_value_set_id,
    segment_name,
    ges_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_fnd_id_flex_segments
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
    FROM transformed_exp_mf_fnd_id_flex_segments
)

SELECT * FROM final