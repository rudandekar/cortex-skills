{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_flex_value_set_struct', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_FLEX_VALUE_SET_STRUCT',
        'target_table': 'STSI_FLEX_VALUE_SET_STRUCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.077057+00:00'
    }
) }}

WITH 

source_ffsi_flex_value_set_struct AS (
    SELECT
        batch_id,
        si_flex_struct_id,
        si_flex_value_set_id,
        si_flex_value_set_desc,
        segment_name,
        db_instance,
        enabled_flag,
        related_flex_struct_id,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ffsi_flex_value_set_struct') }}
),

final AS (
    SELECT
        batch_id,
        si_flex_struct_id,
        si_flex_value_set_id,
        si_flex_value_set_desc,
        segment_name,
        db_instance,
        enabled_flag,
        related_flex_struct_id,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ffsi_flex_value_set_struct
)

SELECT * FROM final