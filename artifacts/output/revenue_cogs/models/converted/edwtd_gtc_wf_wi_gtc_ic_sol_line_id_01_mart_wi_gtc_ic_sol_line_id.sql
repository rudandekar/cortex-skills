{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gtc_ic_sol_line_id', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_WI_GTC_IC_SOL_LINE_ID',
        'target_table': 'WI_GTC_IC_SOL_LINE_ID',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.603094+00:00'
    }
) }}

WITH 

source_wi_gtc_ic_sol_line_id AS (
    SELECT
        line_id,
        solk_line_id,
        top_model_line_id,
        solk_top_model_line_id,
        sol_ss_code,
        ss_code
    FROM {{ source('raw', 'wi_gtc_ic_sol_line_id') }}
),

final AS (
    SELECT
        line_id,
        solk_line_id,
        top_model_line_id,
        solk_top_model_line_id,
        sol_ss_code,
        ss_code
    FROM source_wi_gtc_ic_sol_line_id
)

SELECT * FROM final