{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_n_financial_rli_pc_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_ST_N_FINANCIAL_RLI_PC_HIER',
        'target_table': 'ST_N_FINANCIAL_RLI_PC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.623072+00:00'
    }
) }}

WITH 

source_ff_n_financial_rli_pc_hier AS (
    SELECT
        batch_id,
        parent_rli,
        child_rli,
        child_rli_desc,
        refresh_date,
        create_datetime,
        action_code,
        seq_no
    FROM {{ source('raw', 'ff_n_financial_rli_pc_hier') }}
),

final AS (
    SELECT
        parent_rli,
        child_rli,
        child_rli_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id,
        seq_no
    FROM source_ff_n_financial_rli_pc_hier
)

SELECT * FROM final