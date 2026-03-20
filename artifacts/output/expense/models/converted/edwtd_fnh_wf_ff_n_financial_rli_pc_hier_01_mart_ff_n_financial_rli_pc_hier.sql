{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_n_financial_rli_pc_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_FF_N_FINANCIAL_RLI_PC_HIER',
        'target_table': 'FF_N_FINANCIAL_RLI_PC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.945593+00:00'
    }
) }}

WITH 

source_vw_n_financial_rli_pc_hier AS (
    SELECT
        parent_rli,
        child_rli,
        child_rli_desc,
        refresh_date,
        seq_no
    FROM {{ source('raw', 'vw_n_financial_rli_pc_hier') }}
),

transformed_exptrans AS (
    SELECT
    parent_rli,
    child_rli,
    child_rli_desc,
    refresh_date,
    seq_no,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_vw_n_financial_rli_pc_hier
),

final AS (
    SELECT
        batch_id,
        parent_rli,
        child_rli,
        child_rli_desc,
        refresh_date,
        create_datetime,
        action_code,
        seq_no
    FROM transformed_exptrans
)

SELECT * FROM final