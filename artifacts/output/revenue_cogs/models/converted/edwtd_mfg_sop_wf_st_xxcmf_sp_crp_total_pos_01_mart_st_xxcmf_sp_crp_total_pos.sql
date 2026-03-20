{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_crp_total_pos', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_CRP_TOTAL_POS',
        'target_table': 'ST_XXCMF_SP_CRP_TOTAL_POS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.890042+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_crp_total_pos AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        total_pos,
        creation_date
    FROM {{ source('raw', 'ff_xxcmf_sp_crp_total_pos') }}
),

final AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        total_pos,
        create_date
    FROM source_ff_xxcmf_sp_crp_total_pos
)

SELECT * FROM final