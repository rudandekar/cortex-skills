{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_crp_backlog', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_CRP_BACKLOG',
        'target_table': 'ST_XXCMF_SP_CRP_BACKLOG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.743365+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_crp_backlog AS (
    SELECT
        product_family,
        dimension,
        fiscal_week_start_date,
        buildable,
        unbuildable,
        unstaged,
        staged,
        total,
        creation_date
    FROM {{ source('raw', 'ff_xxcmf_sp_crp_backlog') }}
),

final AS (
    SELECT
        product_family,
        dimension,
        fiscal_week_start_date,
        buildable,
        unbuildable,
        unstaged,
        staged,
        total,
        creation_date
    FROM source_ff_xxcmf_sp_crp_backlog
)

SELECT * FROM final