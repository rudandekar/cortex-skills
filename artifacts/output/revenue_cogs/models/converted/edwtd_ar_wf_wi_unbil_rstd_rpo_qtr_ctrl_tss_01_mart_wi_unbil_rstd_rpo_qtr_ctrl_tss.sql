{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_unbil_rstd_rpo_qtr_ctrl_tss', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_UNBIL_RSTD_RPO_QTR_CTRL_TSS',
        'target_table': 'WI_UNBIL_RSTD_RPO_QTR_CTRL_TSS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.072937+00:00'
    }
) }}

WITH 

source_wi_unbil_rstd_rpo_qtr_ctrl_tss AS (
    SELECT
        fiscal_year_quarter_number_int,
        edw_create_dtm
    FROM {{ source('raw', 'wi_unbil_rstd_rpo_qtr_ctrl_tss') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        edw_create_dtm
    FROM source_wi_unbil_rstd_rpo_qtr_ctrl_tss
)

SELECT * FROM final