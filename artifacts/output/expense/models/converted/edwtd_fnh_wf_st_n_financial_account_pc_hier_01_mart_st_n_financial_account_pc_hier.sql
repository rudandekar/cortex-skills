{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_n_financial_account_pc_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_ST_N_FINANCIAL_ACCOUNT_PC_HIER',
        'target_table': 'ST_N_FINANCIAL_ACCOUNT_PC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.658498+00:00'
    }
) }}

WITH 

source_ff_n_financial_account_pc_hier AS (
    SELECT
        batch_id,
        parent_account,
        child_account,
        child_account_desc,
        refresh_date,
        create_datetime,
        action_code,
        seq_no
    FROM {{ source('raw', 'ff_n_financial_account_pc_hier') }}
),

final AS (
    SELECT
        parent_account,
        child_account,
        child_account_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id,
        seq_no
    FROM source_ff_n_financial_account_pc_hier
)

SELECT * FROM final