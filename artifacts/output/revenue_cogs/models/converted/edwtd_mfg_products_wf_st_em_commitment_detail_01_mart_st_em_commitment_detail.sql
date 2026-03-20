{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_em_commitment_detail', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_EM_COMMITMENT_DETAIL',
        'target_table': 'ST_EM_COMMITMENT_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.838346+00:00'
    }
) }}

WITH 

source_ff_em_commitment_detail AS (
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
    FROM {{ source('raw', 'ff_em_commitment_detail') }}
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
    FROM source_ff_em_commitment_detail
)

SELECT * FROM final