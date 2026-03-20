{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_n_financial_entity_pc_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_ST_N_FINANCIAL_ENTITY_PC_HIER',
        'target_table': 'ST_N_FINANCIAL_ENTITY_PC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.726007+00:00'
    }
) }}

WITH 

source_ff_n_financial_entity_pc_hier AS (
    SELECT
        batch_id,
        parent_entity,
        child_entity,
        child_entity_desc,
        child_entity_type,
        refresh_date,
        create_datetime,
        action_code,
        seq_no
    FROM {{ source('raw', 'ff_n_financial_entity_pc_hier') }}
),

final AS (
    SELECT
        parent_entity,
        child_entity,
        child_entity_desc,
        child_entity_type,
        refresh_date,
        create_datetime,
        action_code,
        batch_id,
        seq_no
    FROM source_ff_n_financial_entity_pc_hier
)

SELECT * FROM final