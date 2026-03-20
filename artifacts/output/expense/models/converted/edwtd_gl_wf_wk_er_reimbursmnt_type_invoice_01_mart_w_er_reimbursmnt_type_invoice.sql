{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_er_reimbursmnt_type_invoice', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_ER_REIMBURSMNT_TYPE_INVOICE',
        'target_table': 'W_ER_REIMBURSMNT_TYPE_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.737872+00:00'
    }
) }}

WITH 

source_n_er_reimbursmnt_type_invoice AS (
    SELECT
        bk_expense_reimbursmnt_typ_cd,
        expense_report_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_er_reimbursmnt_type_invoice') }}
),

source_sm_expense_report AS (
    SELECT
        expense_report_key,
        bk_ss_cd,
        edw_create_user,
        edw_create_dtm,
        sk_report_header_id_int
    FROM {{ source('raw', 'sm_expense_report') }}
),

final AS (
    SELECT
        bk_expense_reimbursmnt_typ_cd,
        expense_report_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_sm_expense_report
)

SELECT * FROM final