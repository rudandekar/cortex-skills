{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcmf_sp_crp_edel_rev_plan', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_FF_XXCMF_SP_CRP_EDEL_REV_PLAN',
        'target_table': 'FF_XXCMF_SP_CRP_EDEL_REV_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.506114+00:00'
    }
) }}

WITH 

source_xxcmf_sp_crp_edel_rev_plan AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        dimension,
        edel_revenue,
        creation_date
    FROM {{ source('raw', 'xxcmf_sp_crp_edel_rev_plan') }}
),

final AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        dimension,
        edel_revenue,
        creation_date
    FROM source_xxcmf_sp_crp_edel_rev_plan
)

SELECT * FROM final