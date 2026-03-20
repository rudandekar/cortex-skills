{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_edw_svc_trg_pl_line_case', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_EDW_SVC_TRG_PL_LINE_CASE',
        'target_table': 'ST_EDW_SVC_TRG_PL_LINE_CASE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.280835+00:00'
    }
) }}

WITH 

source_st_edw_svc_trg_pl_line_case AS (
    SELECT
        fiscal_month,
        case_statement
    FROM {{ source('raw', 'st_edw_svc_trg_pl_line_case') }}
),

final AS (
    SELECT
        fiscal_month,
        case_statement
    FROM source_st_edw_svc_trg_pl_line_case
)

SELECT * FROM final