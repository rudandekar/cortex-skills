{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_fnd_id_flex_segments', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_FND_ID_FLEX_SEGMENTS',
        'target_table': 'ST_CG1_FND_ID_FLEX_SEGMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.647549+00:00'
    }
) }}

WITH 

source_ff_cg1_fnd_id_flex_segments AS (
    SELECT
        application_id,
        id_flex_code,
        id_flex_num,
        application_column_name,
        segment_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        segment_num,
        application_column_index_flag,
        enabled_flag,
        required_flag,
        display_flag,
        display_size,
        security_enabled_flag,
        maximum_description_len,
        concatenation_description_len,
        last_update_login,
        flex_value_set_id,
        range_code,
        default_type,
        default_value,
        runtime_property_function,
        additional_where_clause,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_cg1_fnd_id_flex_segments') }}
),

final AS (
    SELECT
        application_id,
        id_flex_code,
        id_flex_num,
        application_column_name,
        segment_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        segment_num,
        application_column_index_flag,
        enabled_flag,
        required_flag,
        display_flag,
        display_size,
        security_enabled_flag,
        maximum_description_len,
        concatenation_description_len,
        last_update_login,
        flex_value_set_id,
        range_code,
        default_type,
        default_value,
        runtime_property_function,
        additional_where_clause,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_cg1_fnd_id_flex_segments
)

SELECT * FROM final