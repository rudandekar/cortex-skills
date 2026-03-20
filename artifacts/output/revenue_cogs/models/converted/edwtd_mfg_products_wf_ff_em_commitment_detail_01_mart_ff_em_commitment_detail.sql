{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_em_commitment_detail', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_EM_COMMITMENT_DETAIL',
        'target_table': 'FF_EM_COMMITMENT_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.701888+00:00'
    }
) }}

WITH 

source_em_commitment_detail AS (
    SELECT
        commitment_detail_id,
        commitment_id,
        period_start,
        period_end,
        commitment_qty,
        commit_message
    FROM {{ source('raw', 'em_commitment_detail') }}
),

transformed_exp_em_commitment_detail AS (
    SELECT
    commitment_detail_id,
    commitment_id,
    period_start,
    period_end,
    commitment_qty,
    commit_message,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_em_commitment_detail
),

final AS (
    SELECT
        commitment_detail_id,
        commitment_id,
        period_start,
        period_end,
        commitment_qty,
        commit_message,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_em_commitment_detail
)

SELECT * FROM final