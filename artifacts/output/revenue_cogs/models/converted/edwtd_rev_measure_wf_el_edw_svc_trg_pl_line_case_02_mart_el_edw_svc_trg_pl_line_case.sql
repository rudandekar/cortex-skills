{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_edw_svc_trg_pl_line_case', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_EDW_SVC_TRG_PL_LINE_CASE',
        'target_table': 'EL_EDW_SVC_TRG_PL_LINE_CASE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.426007+00:00'
    }
) }}

WITH 

source_wi_edw_svc_trg_pl_line_case AS (
    SELECT
        fiscal_month,
        sql_value
    FROM {{ source('raw', 'wi_edw_svc_trg_pl_line_case') }}
),

source_el_edw_svc_trg_pl_line_case AS (
    SELECT
        fiscal_month,
        case_statement,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_edw_svc_trg_pl_line_case') }}
),

final AS (
    SELECT
        fiscal_month,
        case_statement,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_el_edw_svc_trg_pl_line_case
)

SELECT * FROM final