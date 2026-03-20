{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcmf_sp_crp_total_shpd_rev', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_FF_XXCMF_SP_CRP_TOTAL_SHPD_REV',
        'target_table': 'FF_XXCMF_SP_CRP_TOTAL_SHPD_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.961256+00:00'
    }
) }}

WITH 

source_xxcmf_sp_crp_total_shpd_rev AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        direct_ship_revenue,
        acquisition_ce,
        creation_date
    FROM {{ source('raw', 'xxcmf_sp_crp_total_shpd_rev') }}
),

final AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        direct_ship_revenue,
        acquisition_ce,
        creation_date
    FROM source_xxcmf_sp_crp_total_shpd_rev
)

SELECT * FROM final