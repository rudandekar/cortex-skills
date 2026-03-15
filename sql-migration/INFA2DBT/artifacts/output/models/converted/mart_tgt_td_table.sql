{{ config(
    materialized='table',
    schema='tgt_db',
    tags=['wf_m_td_to_td', 'TODO_freq', 'TODO_domain'],
    meta={
        'source_workflow': 'wf_m_TD_to_TD',
        'target_table': 'TGT_TD_TABLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-15T13:12:35.099961+00:00'
    }
) }}

WITH 

source_src_td_table AS (
    SELECT
        col1,
        col2
    FROM {{ source('raw', 'src_td_table') }}
),

transformed_exp_passthru AS (
    SELECT
    col1_in,
    col2_in,
    COL1 AS col1,
    COL2 AS col2
    FROM source_src_td_table
),

final AS (
    SELECT
        col1,
        col2
    FROM transformed_exp_passthru
)

SELECT * FROM final