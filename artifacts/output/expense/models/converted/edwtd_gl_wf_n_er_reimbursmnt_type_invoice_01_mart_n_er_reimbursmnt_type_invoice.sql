{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_er_reimbursmnt_type_invoice', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_ER_REIMBURSMNT_TYPE_INVOICE',
        'target_table': 'N_ER_REIMBURSMNT_TYPE_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.749983+00:00'
    }
) }}

WITH 

source_w_er_reimbursmnt_type_invoice AS (
    SELECT
        bk_expense_reimbursmnt_typ_cd,
        expense_report_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_er_reimbursmnt_type_invoice') }}
),

final AS (
    SELECT
        bk_expense_reimbursmnt_typ_cd,
        expense_report_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_er_reimbursmnt_type_invoice
)

SELECT * FROM final