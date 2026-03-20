{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_unbil_rstd_rpo_eng_mth_ctrl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_UNBIL_RSTD_RPO_ENG_MTH_CTRL',
        'target_table': 'WI_UNBIL_RSTD_RPO_ENG_MTH_CTRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.988084+00:00'
    }
) }}

WITH 

source_wi_unbil_rstd_rpo_eng_mth_ctrl AS (
    SELECT
        fiscal_year_mth_int,
        edw_create_dtm
    FROM {{ source('raw', 'wi_unbil_rstd_rpo_eng_mth_ctrl') }}
),

final AS (
    SELECT
        fiscal_year_mth_int,
        edw_create_dtm
    FROM source_wi_unbil_rstd_rpo_eng_mth_ctrl
)

SELECT * FROM final