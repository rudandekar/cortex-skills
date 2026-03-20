{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_crp_true_demand', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_CRP_TRUE_DEMAND',
        'target_table': 'ST_XXCMF_SP_CRP_TRUE_DEMAND',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.528826+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_crp_true_demand AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        true_demand_plan,
        net_pos_plan,
        creation_date
    FROM {{ source('raw', 'ff_xxcmf_sp_crp_true_demand') }}
),

final AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        true_demand_plan,
        net_pos_plan,
        creation_date
    FROM source_ff_xxcmf_sp_crp_true_demand
)

SELECT * FROM final