{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sr_volume', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SR_VOLUME',
        'target_table': 'WI_SR_VOLUME',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.698167+00:00'
    }
) }}

WITH 

source_wi_sr_volume AS (
    SELECT
        initial_tech_id,
        initial_sub_tech_id,
        initial_tech_name,
        initial_dw_crnt_keyword_key,
        final_tech_id,
        final_sub_tech_id,
        final_tech_name,
        final_dw_crnt_keyword_key,
        problem_code
    FROM {{ source('raw', 'wi_sr_volume') }}
),

final AS (
    SELECT
        initial_tech_id,
        initial_sub_tech_id,
        initial_tech_name,
        initial_dw_crnt_keyword_key,
        final_tech_id,
        final_sub_tech_id,
        final_tech_name,
        final_dw_crnt_keyword_key,
        problem_code
    FROM source_wi_sr_volume
)

SELECT * FROM final