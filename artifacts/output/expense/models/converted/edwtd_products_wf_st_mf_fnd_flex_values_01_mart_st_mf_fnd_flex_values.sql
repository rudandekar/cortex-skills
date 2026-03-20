{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fnd_flex_values', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FND_FLEX_VALUES',
        'target_table': 'ST_MF_FND_FLEX_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.649440+00:00'
    }
) }}

WITH 

source_ff_mf_fnd_flex_values AS (
    SELECT
        enabled_flag,
        flex_value,
        flex_value_id,
        flex_value_set_id,
        global_name,
        start_date_active,
        end_date_active,
        batch_id,
        create_datetime,
        ges_update_date,
        action_code,
        description
    FROM {{ source('raw', 'ff_mf_fnd_flex_values') }}
),

final AS (
    SELECT
        batch_id,
        enabled_flag,
        flex_value,
        flex_value_id,
        flex_value_set_id,
        global_name,
        start_date_active,
        end_date_active,
        create_datetime,
        ges_update_date,
        action_code,
        description
    FROM source_ff_mf_fnd_flex_values
)

SELECT * FROM final