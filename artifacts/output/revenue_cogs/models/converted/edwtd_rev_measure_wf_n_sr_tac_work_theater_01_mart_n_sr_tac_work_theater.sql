{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sr_tac_work_theater', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_SR_TAC_WORK_THEATER',
        'target_table': 'N_SR_TAC_WORK_THEATER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.725142+00:00'
    }
) }}

WITH 

source_n_sr_tac_work_theater AS (
    SELECT
        sr_tac_work_theater_code,
        work_theater_is_valid_flag,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sr_tac_work_theater') }}
),

final AS (
    SELECT
        sr_tac_work_theater_code,
        work_theater_is_valid_flag,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_sr_tac_work_theater
)

SELECT * FROM final