{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_flex_value_set_struct', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_FLEX_VALUE_SET_STRUCT',
        'target_table': 'FFSI_FLEX_VALUE_SET_STRUCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.779137+00:00'
    }
) }}

WITH 

source_si_flex_value_set_struct AS (
    SELECT
        si_flex_struct_id,
        si_flex_value_set_id,
        si_flex_value_set_desc,
        segment_name,
        db_instance,
        enabled_flag,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        related_flex_struct_id
    FROM {{ source('raw', 'si_flex_value_set_struct') }}
),

transformed_exp_set_default_values AS (
    SELECT
    si_flex_struct_id,
    last_update_date,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_si_flex_value_set_struct
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
    FROM transformed_exp_set_default_values
)

SELECT * FROM final